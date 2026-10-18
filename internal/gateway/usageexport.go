package gateway

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
)

// usageColumns 是用量导出的表头，每次调用一行。重放的行 replay_of 是原来的调用号，嵌入缓存命中的行 cached_from 是原来的调用号，
// 这两种的 usage 三列为空（没调上游）。新列只加在最后：按列号读的脚本不受影响。
var usageColumns = []string{
	"time", "request_id", "client_request_id", "run_id", "endpoint", "model_alias", "upstream_model",
	"status", "error_code", "prompt_tokens", "completion_tokens", "total_tokens", "duration_ms", "replay_of", "cached_from",
}

// callLine 是 logCall 写出的一行里导出要用的字段。
type callLine struct {
	Msg             string          `json:"msg"`
	Time            string          `json:"time"`
	RequestID       string          `json:"request_id"`
	ClientRequestID string          `json:"client_request_id"`
	RunID           string          `json:"run_id"`
	Endpoint        string          `json:"endpoint"`
	ModelAlias      string          `json:"model_alias"`
	UpstreamModel   string          `json:"upstream_model"`
	Status          int             `json:"status"`
	ErrorCode       string          `json:"error_code"`
	Usage           json.RawMessage `json:"usage"`
	DurationMS      int64           `json:"duration_ms"`
	ReplayOf        string          `json:"replay_of"`
	CachedFrom      string          `json:"cached_from"`
}

var errNotCall = errors.New("not a call line")

// ExportUsage 从 QF 的日志（每行一个 JSON）里取出调用行，写成 CSV。
// usage 的三列：日志记 unknown 就写 unknown，没调上游就留空，嵌入没有 completion_tokens 也留空，从不补 0。
// 不是调用的日志行（例如启动日志）跳过；解析不了的行跳过并计数，由调用方报出来。
// 每个单元格都经过 textCell，表格软件打开时不会把客户端或上游给的值当成公式。
func ExportUsage(r io.Reader, w io.Writer) (invalid int, err error) {
	out := csv.NewWriter(w)
	if err := out.Write(usageColumns); err != nil {
		return 0, err
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		row, err := usageRow(sc.Bytes())
		switch {
		case errors.Is(err, errNotCall):
			continue
		case err != nil:
			invalid++
			continue
		}
		for i := range row {
			row[i] = textCell(row[i])
		}
		if err := out.Write(row); err != nil {
			return invalid, err
		}
	}
	if err := sc.Err(); err != nil {
		return invalid, err
	}
	out.Flush()
	return invalid, out.Error()
}

func usageRow(line []byte) ([]string, error) {
	var l callLine
	if err := json.Unmarshal(line, &l); err != nil {
		return nil, err
	}
	if l.Msg != "call" {
		return nil, errNotCall
	}
	if l.RequestID == "" {
		return nil, errors.New("call line without request_id")
	}
	prompt, completion, total, err := usageCells(l.Usage)
	if err != nil {
		return nil, err
	}
	status := ""
	if l.Status != 0 {
		status = strconv.Itoa(l.Status)
	}
	return []string{
		l.Time, l.RequestID, l.ClientRequestID, l.RunID, l.Endpoint, l.ModelAlias, l.UpstreamModel,
		status, l.ErrorCode, prompt, completion, total, strconv.FormatInt(l.DurationMS, 10), l.ReplayOf, l.CachedFrom,
	}, nil
}

// usageCells 把日志里的 usage 换成三列：没有这个字段（没调上游）是空，"unknown" 原样，对象照抄数字（和对账读日志同一个
// decodeUsage）。
func usageCells(raw json.RawMessage) (prompt, completion, total string, err error) {
	if raw == nil {
		return "", "", "", nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if s != "unknown" {
			return "", "", "", errors.New("usage string is not unknown")
		}
		return "unknown", "unknown", "unknown", nil
	}
	u := decodeUsage(raw)
	if u == nil {
		return "", "", "", errors.New("usage is not an object with prompt_tokens and total_tokens")
	}
	if u.HasCompletion {
		completion = strconv.FormatInt(u.Completion, 10)
	}
	return strconv.FormatInt(u.Prompt, 10), completion, strconv.FormatInt(u.Total, 10), nil
}

// formulaPrefixes 是表格软件可能当成公式开头的字符，加上单引号本身（为了能还原）。
const formulaPrefixes = "=+-@\t\r'"

// textCell 让一个单元格在 Excel 这类软件里只当文本：以 formulaPrefixes 里的字符开头的值，前面加一个单引号。
// 别名、客户端请求号、run_id、上游的模型名都可能以这些字符开头；QF 自己的值（时间、调用号、接口、状态、错误码、
// 用量、时长）不会，所以原样。读回时去掉一个前导单引号就是原值。
func textCell(s string) string {
	if s != "" && strings.ContainsRune(formulaPrefixes, rune(s[0])) {
		return "'" + s
	}
	return s
}
