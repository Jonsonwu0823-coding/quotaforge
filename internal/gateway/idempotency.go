package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Jonsonwu0823-coding/quotaforge/internal/config"
)

// Idempotency-Key 的值：1 到 128 个可见 ASCII 字符，按字节比较。值是不透明的，QF 不解析引号。
const maxIdempotencyKeyLen = 128

// idempotencyKey 校验 Idempotency-Key 头。没带返回空串；带了但不是恰好一个合法的值，返回 400。
// 不合法的键不能悄悄忽略：客户端会以为有幂等，重试却会重复扣费。
func idempotencyKey(h http.Header) (string, *apiError) {
	values, ok := h[headerIdempotencyKey]
	switch {
	case !ok:
		return "", nil
	case len(values) != 1 || len(values[0]) == 0 || len(values[0]) > maxIdempotencyKeyLen:
		return "", errInvalidIdempotencyKey
	}
	for _, b := range []byte(values[0]) {
		if b < 0x21 || b > 0x7e {
			return "", errInvalidIdempotencyKey
		}
	}
	return values[0], nil
}

// fingerprint 是请求指纹：SHA-256（接口 + 0 字节 + 规范化的原始请求体）。请求头都不算。
func fingerprint(p config.Purpose, body []byte) ([]byte, error) {
	canonical, err := canonicalJSON(body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(slices.Concat([]byte(p), []byte{0}, canonical))
	return sum[:], nil
}

// canonicalJSON 返回 JSON 的规范形式，用来判断两个请求体是不是同一个请求：
//   - 去掉无意义的空白；字符串解码再编码，转义的写法统一；数组保持顺序；
//   - 对象的成员按折叠后的键（见 foldKey）的字节序稳定排序；折叠后相同的键（重复的、只差大小写的、
//     Unicode 折叠相同的）都保留、保持原来的先后：上游可能取前一个也可能取后一个，也可能不区分大小写，
//     去重或者给它们排出固定的先后，都可能把上游眼里不同的请求当成同一个；
//   - 数字按字面，不转成浮点：转成浮点会让大整数（例如 seed）失真，代价是 1 和 1.0 算两个请求。
//
// 宁可把同一个请求当成不同的（重试得到 422），也不把不同的请求当成同一个（错误地重放）。所以非法 UTF-8 和
// 孤立的代理项转义（例如 \ud800）返回错误：它们解码后都变成 U+FFFD，不同的请求会撞成同一个。
func canonicalJSON(body []byte) ([]byte, error) {
	if !utf8.Valid(body) {
		return nil, errors.New("body is not valid UTF-8")
	}
	if hasLoneSurrogate(body) {
		return nil, errors.New("body has an unpaired surrogate escape")
	}
	c := canonicalizer{dec: json.NewDecoder(bytes.NewReader(body))}
	c.dec.UseNumber()
	var objects []*object
	if err := c.value(&objects); err != nil {
		return nil, err
	}
	if _, err := c.dec.Token(); err != io.EOF {
		return nil, errors.New("trailing data after the JSON value")
	}
	return c.appendRange(make([]byte, 0, len(c.buf)), 0, len(c.buf), objects), nil
}

// canonicalizer 一边读，一边把规范形式按请求体里的顺序写进 buf；对象的成员先不排，只记下每个成员在 buf 里的范围。
// 读完以后 appendRange 按排好的顺序把这些范围拼起来，每个字节只复制一次。这样用时和请求体的大小成线性，
// 和嵌套多深无关：带键的请求在额度检查之前就要算指纹，如果每层对象都把里面的值复制一遍，深嵌套、里层很大的
// 请求体会在拒绝之前占很多 CPU。
type canonicalizer struct {
	dec *json.Decoder
	buf []byte
}

// object 是一个对象：它在 buf 里占 [start, end)（成员按请求体里的顺序，没有括号和逗号），members 已排好序。
type object struct {
	start, end int
	members    []member
}

// member 是对象的一个成员："键":值 在 buf 里占 [start, end)；objects 是值里直接嵌套的对象（不含更深的），按出现的先后。
type member struct {
	folded     string
	start, end int
	objects    []*object
}

// value 读一个完整的 JSON 值，写进 buf；值里直接嵌套的对象记进 objects。
func (c *canonicalizer) value(objects *[]*object) error {
	tok, err := c.dec.Token()
	if err != nil {
		return err
	}
	switch v := tok.(type) {
	case json.Delim: // [ 或 {；Token 已经保证配对
		if err := c.container(v, objects); err != nil {
			return err
		}
		_, err := c.dec.Token() // 结尾的 ] 或 }
		return err
	case string:
		c.buf = appendString(c.buf, v)
	case json.Number:
		c.buf = append(c.buf, v...)
	case bool:
		c.buf = strconv.AppendBool(c.buf, v)
	case nil:
		c.buf = append(c.buf, "null"...)
	}
	return nil
}

// container 读一个数组或对象的内容（开头的 [ 或 { 已经读过，结尾的不读）：数组按原来的顺序写进 buf，对象记进 objects。
func (c *canonicalizer) container(open json.Delim, objects *[]*object) error {
	if open == '{' {
		o, err := c.object()
		if err != nil {
			return err
		}
		*objects = append(*objects, o)
		return nil
	}
	c.buf = append(c.buf, '[')
	for i := 0; c.dec.More(); i++ {
		if i > 0 {
			c.buf = append(c.buf, ',')
		}
		if err := c.value(objects); err != nil {
			return err
		}
	}
	c.buf = append(c.buf, ']')
	return nil
}

// object 读一个对象的成员（开头的 { 已经读过），按折叠后的键稳定排序。
func (c *canonicalizer) object() (*object, error) {
	o := &object{start: len(c.buf)}
	for c.dec.More() {
		key, err := c.dec.Token()
		if err != nil {
			return nil, err
		}
		m := member{start: len(c.buf)}
		c.buf = append(appendString(c.buf, key.(string)), ':')
		if err := c.value(&m.objects); err != nil {
			return nil, err
		}
		m.folded, m.end = foldKey(key.(string)), len(c.buf)
		o.members = append(o.members, m)
	}
	o.end = len(c.buf)
	slices.SortStableFunc(o.members, func(a, b member) int { return strings.Compare(a.folded, b.folded) })
	return o, nil
}

// appendRange 把 buf[start:end] 写进 out，其中的对象（objects）换成按排好的成员写出的 {…}。
func (c *canonicalizer) appendRange(out []byte, start, end int, objects []*object) []byte {
	for _, o := range objects {
		out = append(append(out, c.buf[start:o.start]...), '{')
		for i, m := range o.members {
			if i > 0 {
				out = append(out, ',')
			}
			out = c.appendRange(out, m.start, m.end, m.objects)
		}
		out = append(out, '}')
		start = o.end
	}
	return append(out, c.buf[start:end]...)
}

// foldKey 把键里每个字符换成它的 unicode.SimpleFold 环里最小的那个。有的解码器（例如 Go 的 encoding/json
// 解到结构体）认键名时不区分大小写、做 Unicode 简单折叠，后出现的为准：{"Temperature":0.1,"temperature":0.9}
// 和换了先后的写法，它读到的值不同。所以排序只看折叠后的键，折叠后相同的成员保持请求体里的先后，
// 不能再按原始字节分先后，否则这两种写法又会排成同一个规范形式。
func foldKey(key string) string {
	return strings.Map(func(r rune) rune {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			least = min(least, f)
		}
		return least
	}, key)
}

func appendString(out []byte, s string) []byte {
	enc, _ := json.Marshal(s) // 合法 UTF-8 的字符串编码不会失败
	return append(out, enc...)
}

// hasLoneSurrogate 报告请求体里有没有孤立的代理项转义：高位后面没跟低位，或者低位单独出现。
// 合法的 JSON 里反斜杠只出现在字符串里，所以从头扫一遍，每个反斜杠连同它的转义一起跳过就行。
func hasLoneSurrogate(b []byte) bool {
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		r, ok := unicodeEscape(b, i)
		switch {
		case !ok: // 其它转义：跳过反斜杠后面那个字符
			i++
		case r >= 0xd800 && r < 0xdc00:
			if low, ok := unicodeEscape(b, i+6); !ok || low < 0xdc00 || low >= 0xe000 {
				return true
			}
			i += 11
		case r >= 0xdc00 && r < 0xe000:
			return true
		default:
			i += 5
		}
	}
	return false
}

// unicodeEscape 读 b[i:] 开头的 \uXXXX。
func unicodeEscape(b []byte, i int) (rune, bool) {
	if i+6 > len(b) || b[i] != '\\' || b[i+1] != 'u' {
		return 0, false
	}
	n, err := strconv.ParseUint(string(b[i+2:i+6]), 16, 32)
	return rune(n), err == nil
}
