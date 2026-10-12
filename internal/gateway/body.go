package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// maxMaxTokens 是 max_tokens 的上限。预留量 = 请求体字节数 + max_tokens，限住它就不会溢出。
const maxMaxTokens = 1<<31 - 1

// parsedRequest 是客户端请求体里 QF 需要的字段，以及顶层 model 值在原始字节里的位置。
type parsedRequest struct {
	model                string
	stream               bool
	maxTokens            *int64 // nil：没写或写了 null
	modelStart, modelEnd int
}

// parseRequest 只读顶层的 model、stream 和 max_tokens，其它字段不解码、不校验，原样交给上游判断。
// 不能整体解码再编码：encoding/json 会重排键、压缩空白、转义 <>&，tools 就不是原样了。
// 所以逐个读顶层键，记下 model 值的字节范围，之后只替换这一段。
func parseRequest(body []byte) (parsedRequest, error) {
	var p parsedRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return p, errors.New("body is not a JSON object")
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return p, err
		}
		var raw json.RawMessage // 每个值都完整走一遍，顺带校验 JSON 语法
		if err := dec.Decode(&raw); err != nil {
			return p, err
		}
		// QF 读的字段，QF 和上游必须读到同一个值，否则查的别名和实际调用的模型对不上，
		// QF 当成非流式、上游却按流式回答，或者 QF 按小的 max_tokens 预留、上游却按大的生成。
		// 有的上游（例如 Go 的 encoding/json）解析键名时不区分大小写、做 Unicode 简单折叠，
		// 后出现的键覆盖先出现的。所以和这些字段折叠后相同的键，只接受全小写原样、只能出现一次；
		// MODEL、Model、ſtream、MAX_TOKENS 这类写法和重复一样拒绝。
		key := tok.(string)
		field := ""
		for _, f := range []string{"model", "stream", "max_tokens"} {
			if strings.EqualFold(key, f) {
				field = f
			}
		}
		if field != "" {
			if key != field || seen[field] {
				return p, errors.New("ambiguous " + field + " field")
			}
			seen[field] = true
		}
		switch field {
		case "model":
			if raw[0] != '"' {
				return p, errors.New("model is not a string")
			}
			if err := json.Unmarshal(raw, &p.model); err != nil {
				return p, err
			}
			// RawMessage 不含值前面的空白，所以值的起点是终点减去它的长度。
			p.modelEnd = int(dec.InputOffset())
			p.modelStart = p.modelEnd - len(raw)
		case "stream":
			if err := json.Unmarshal(raw, &p.stream); err != nil {
				return p, err
			}
		case "max_tokens":
			if p.maxTokens, err = parseMaxTokens(raw); err != nil {
				return p, err
			}
		}
	}
	if _, err := dec.Token(); err != nil { // 结尾的 }
		return p, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return p, errors.New("trailing data after the JSON object")
	}
	if p.model == "" {
		return p, errors.New("model is missing or empty")
	}
	return p, nil
}

// parseMaxTokens 要求 JSON 整数（1.5、1e3、"8" 都不行），0 ≤ 值 ≤ maxMaxTokens；null 当作没写。
func parseMaxTokens(raw json.RawMessage) (*int64, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, err
	}
	if n < 0 || n > maxMaxTokens {
		return nil, errors.New("max_tokens is out of range")
	}
	return &n, nil
}

// withModel 返回把顶层 model 的值换成 name 之后的请求体，其余字节一字不改。
func (p parsedRequest) withModel(body []byte, name string) []byte {
	enc, _ := json.Marshal(name) // 字符串编码不会失败
	out := make([]byte, 0, len(body)-(p.modelEnd-p.modelStart)+len(enc))
	out = append(out, body[:p.modelStart]...)
	out = append(out, enc...)
	return append(out, body[p.modelEnd:]...)
}
