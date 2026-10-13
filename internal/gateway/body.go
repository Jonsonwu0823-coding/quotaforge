package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strings"
)

// maxMaxTokens 是 max_tokens 的上限。预留量 = 请求体字节数 + max_tokens，限住它就不会溢出。
const maxMaxTokens = 1<<31 - 1

// invalidReason 是 400 invalid_request 的原因，进调用日志的 invalid_reason 字段，响应不变。取值只有下面这几个：
// encoding/json 的错误原文会带上请求体里的字符，不能进日志。
type invalidReason string

const (
	reasonUnreadableBody    invalidReason = "unreadable_body"    // 读请求体失败（例如分块编码坏了）
	reasonNotObject         invalidReason = "not_object"         // 不是 JSON 对象
	reasonMalformedJSON     invalidReason = "malformed_json"     // 对象里的 JSON 语法不对
	reasonTrailingData      invalidReason = "trailing_data"      // 对象后面还有数据
	reasonAmbiguousField    invalidReason = "ambiguous_field"    // model、stream、max_tokens 重复或只差大小写
	reasonModelNotString    invalidReason = "model_not_string"   // model 不是字符串
	reasonModelMissing      invalidReason = "model_missing"      // 没有 model 或为空
	reasonStreamNotBoolean  invalidReason = "stream_not_boolean" // stream 不是布尔值或 null
	reasonMaxTokensInvalid  invalidReason = "max_tokens_invalid" // max_tokens 不是整数或超出范围
	reasonFingerprintFailed invalidReason = "fingerprint_failed" // 带幂等键时算不出指纹（非法 UTF-8、孤立的代理项转义）
	// 流式请求的 stream_options 不是对象或 null，或者其中的 include_usage 不是布尔值或 null
	reasonStreamOptionsInvalid invalidReason = "stream_options_invalid"
)

// parsedRequest 是客户端请求体里 QF 需要的字段，以及改写时要用的字节位置。
type parsedRequest struct {
	model                string
	stream               bool
	maxTokens            *int64 // nil：没写或写了 null
	modelStart, modelEnd int
	objectStart          int // 顶层 { 之后的位置

	// 顶层的 stream_options：只在流式时用（见 checkStreamOptions），非流式不看、不校验。
	streamOptions       json.RawMessage // 没写时为 nil
	streamOptionsEnd    int             // 值在请求体里的终点
	streamOptionsUnsure bool            // 重复，或者有只差大小写、Unicode 折叠的变体键

	usage      *edit // 流式时让上游给 usage 的改写；客户端自己要了 usage 时为 nil
	wantsUsage bool  // 客户端自己写了 include_usage: true
}

// edit 是对请求体的一处改写：[start, end) 换成 text。
type edit struct {
	start, end int
	text       string
}

// parseRequest 只读顶层的 model、stream 和 max_tokens，其它字段不解码、不校验，原样交给上游判断。不合格时返回原因，合格时返回空串。
// 不能整体解码再编码：encoding/json 会重排键、压缩空白、转义 <>&，tools 就不是原样了。
// 所以逐个读顶层键，记下 model 值的字节范围，之后只替换这一段。
func parseRequest(body []byte) (parsedRequest, invalidReason) {
	var p parsedRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return p, reasonNotObject
	}
	p.objectStart = int(dec.InputOffset())
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return p, reasonMalformedJSON
		}
		var raw json.RawMessage // 每个值都完整走一遍，顺带校验 JSON 语法
		if err := dec.Decode(&raw); err != nil {
			return p, reasonMalformedJSON
		}
		if reason := p.member(tok.(string), raw, int(dec.InputOffset()), seen); reason != "" {
			return p, reason
		}
	}
	if _, err := dec.Token(); err != nil { // 结尾的 }
		return p, reasonMalformedJSON
	}
	if _, err := dec.Token(); err != io.EOF {
		return p, reasonTrailingData
	}
	if p.model == "" {
		return p, reasonModelMissing
	}
	return p, ""
}

// member 处理顶层的一个成员：QF 读的字段（model、stream、max_tokens）记下来，别的不看。end 是值在请求体里的终点。
//
// QF 读的字段，QF 和上游必须读到同一个值，否则查的别名和实际调用的模型对不上，
// QF 当成非流式、上游却按流式回答，或者 QF 按小的 max_tokens 预留、上游却按大的生成。
// 有的上游（例如 Go 的 encoding/json）解析键名时不区分大小写、做 Unicode 简单折叠，
// 后出现的键覆盖先出现的。所以和这些字段折叠后相同的键，只接受全小写原样、只能出现一次；
// MODEL、Model、ſtream、MAX_TOKENS 这类写法和重复一样拒绝。
func (p *parsedRequest) member(key string, raw json.RawMessage, end int, seen map[string]bool) invalidReason {
	if p.noteStreamOptions(key, raw, end) {
		return ""
	}
	field := ""
	for _, f := range []string{"model", "stream", "max_tokens"} {
		if strings.EqualFold(key, f) {
			field = f
		}
	}
	if field == "" {
		return ""
	}
	if key != field || seen[field] {
		return reasonAmbiguousField
	}
	seen[field] = true
	switch field {
	case "model":
		if raw[0] != '"' || json.Unmarshal(raw, &p.model) != nil {
			return reasonModelNotString
		}
		// RawMessage 不含值前面的空白，所以值的起点是终点减去它的长度。
		p.modelStart, p.modelEnd = end-len(raw), end
	case "stream":
		if json.Unmarshal(raw, &p.stream) != nil {
			return reasonStreamNotBoolean
		}
	default: // max_tokens
		var ok bool
		if p.maxTokens, ok = parseMaxTokens(raw); !ok {
			return reasonMaxTokensInvalid
		}
	}
	return ""
}

// noteStreamOptions 记下顶层的 stream_options（和它折叠后相同的键也算），只在流式时检查（checkStreamOptions）。
// key 不是它时返回 false。
func (p *parsedRequest) noteStreamOptions(key string, raw json.RawMessage, end int) bool {
	if !strings.EqualFold(key, "stream_options") {
		return false
	}
	p.streamOptionsUnsure = p.streamOptionsUnsure || key != "stream_options" || p.streamOptions != nil
	p.streamOptions, p.streamOptionsEnd = raw, end
	return true
}

// parseMaxTokens 要求 JSON 整数（1.5、1e3、"8" 都不行），0 ≤ 值 ≤ maxMaxTokens；null 当作没写。不合格时 ok 为 false。
func parseMaxTokens(raw json.RawMessage) (n *int64, ok bool) {
	if string(raw) == "null" {
		return nil, true
	}
	var v int64
	if json.Unmarshal(raw, &v) != nil || v < 0 || v > maxMaxTokens {
		return nil, false
	}
	return &v, true
}

// streamOptions 检查流式请求的 stream_options，定下让上游在流的末尾给 usage 的改写（p.usage）：QF 要按它结算。
// 客户端没要 usage（没写、null、include_usage 是 false 或 null）时，发给上游的改成 include_usage: true，
// 客户端收到的流里再去掉 usage 那个事件（见 relay）；stream_options 的其它成员不动。
// stream_options 和 include_usage 照 member 的规则：重复、大小写和 Unicode 折叠的变体都拒绝，否则 QF 和上游可能读到不同的值。
func (p *parsedRequest) checkStreamOptions() invalidReason {
	const usageMember = `"include_usage":true`
	raw := p.streamOptions
	switch {
	case p.streamOptionsUnsure:
		return reasonAmbiguousField
	case raw == nil:
		p.usage = &edit{p.objectStart, p.objectStart, `"stream_options":{` + usageMember + `},`}
		return ""
	case string(raw) == "null":
		p.usage = &edit{p.streamOptionsEnd - len(raw), p.streamOptionsEnd, "{" + usageMember + "}"}
		return ""
	case raw[0] != '{':
		return reasonStreamOptionsInvalid
	}
	value, end, reason := includeUsage(raw)
	switch {
	case reason != "":
		return reason
	case value == nil: // 对象里没有 include_usage：插在 { 之后
		start := p.streamOptionsEnd - len(raw) + 1
		text := usageMember
		if len(bytes.TrimSpace(raw[1:len(raw)-1])) > 0 {
			text += ","
		}
		p.usage = &edit{start, start, text}
	case string(value) == "true":
		p.wantsUsage = true
	case string(value) == "false" || string(value) == "null":
		end += p.streamOptionsEnd - len(raw)
		p.usage = &edit{end - len(value), end, "true"}
	default:
		return reasonStreamOptionsInvalid
	}
	return ""
}

// includeUsage 在 stream_options 对象里找 include_usage，返回它的值和值在对象里的终点；没有时值为 nil。
// 对象本身的语法 parseRequest 已经校验过。
func includeUsage(obj json.RawMessage) (value json.RawMessage, end int, reason invalidReason) {
	dec := json.NewDecoder(bytes.NewReader(obj))
	_, _ = dec.Token() // {
	for dec.More() {
		tok, _ := dec.Token()
		var raw json.RawMessage
		_ = dec.Decode(&raw)
		key := tok.(string)
		if !strings.EqualFold(key, "include_usage") {
			continue
		}
		if key != "include_usage" || value != nil {
			return nil, 0, reasonAmbiguousField
		}
		value, end = raw, int(dec.InputOffset())
	}
	return value, end, ""
}

// upstreamBody 返回发给上游的请求体：顶层 model 的值换成 name，流式时再加上 p.usage，其余字节一字不改。
func (p parsedRequest) upstreamBody(body []byte, name string) []byte {
	enc, _ := json.Marshal(name) // 字符串编码不会失败
	edits := []edit{{p.modelStart, p.modelEnd, string(enc)}}
	if p.usage != nil {
		edits = append(edits, *p.usage)
	}
	slices.SortFunc(edits, func(a, b edit) int { return a.start - b.start }) // 两处改写不重叠：model 不在 stream_options 里
	out := make([]byte, 0, len(body)+64)
	last := 0
	for _, e := range edits {
		out = append(append(out, body[last:e.start]...), e.text...)
		last = e.end
	}
	return append(out, body[last:]...)
}
