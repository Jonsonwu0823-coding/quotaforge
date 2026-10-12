package gateway

import (
	"bytes"
	"encoding/json"
	"io"
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
)

// parsedRequest 是客户端请求体里 QF 需要的字段，以及顶层 model 值在原始字节里的位置。
type parsedRequest struct {
	model                string
	stream               bool
	maxTokens            *int64 // nil：没写或写了 null
	modelStart, modelEnd int
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

// withModel 返回把顶层 model 的值换成 name 之后的请求体，其余字节一字不改。
func (p parsedRequest) withModel(body []byte, name string) []byte {
	enc, _ := json.Marshal(name) // 字符串编码不会失败
	out := make([]byte, 0, len(body)-(p.modelEnd-p.modelStart)+len(enc))
	out = append(out, body[:p.modelStart]...)
	out = append(out, enc...)
	return append(out, body[p.modelEnd:]...)
}
