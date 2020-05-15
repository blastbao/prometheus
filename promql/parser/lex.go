// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Item represents a token or text string returned from the scanner.
//
// Item 代表从 scanner 返回的 token 或 文本字符串。
//
type Item struct {
	Typ ItemType // The type of this Item.
	Pos Pos      // The starting position, in bytes, of this Item in the input string.
	Val string   // The value of this Item.
}

// String returns a descriptive string for the Item.
//
// String 返回 Item 的描述性字符串。
//
func (i Item) String() string {
	switch {
	// 结束符
	case i.Typ == EOF:
		return "EOF"
	// 错误
	case i.Typ == ERROR:
		return i.Val
	// 标识符
	case i.Typ == IDENTIFIER || i.Typ == METRIC_IDENTIFIER:
		return fmt.Sprintf("%q", i.Val)
	// 关键字
	case i.Typ.IsKeyword():
		return fmt.Sprintf("<%s>", i.Val)
	// 操作符
	case i.Typ.IsOperator():
		return fmt.Sprintf("<op:%s>", i.Val)
	// 聚合操作符
	case i.Typ.IsAggregator():
		return fmt.Sprintf("<aggr:%s>", i.Val)
	//
	case len(i.Val) > 10:
		return fmt.Sprintf("%.10q...", i.Val)
	}
	return fmt.Sprintf("%q", i.Val)
}

func (i Item) desc() string {
	if _, ok := ItemTypeStr[i.Typ]; ok {
		return i.String()
	}
	if i.Typ == EOF {
		return i.Typ.desc()
	}
	return fmt.Sprintf("%s %s", i.Typ.desc(), i)
}

type ItemType int

// isOperator returns true if the Item corresponds to a arithmetic or set operator.
// Returns false otherwise.
func (i ItemType) IsOperator() bool { return i > operatorsStart && i < operatorsEnd }

// isAggregator returns true if the Item belongs to the aggregator functions.
// Returns false otherwise
func (i ItemType) IsAggregator() bool { return i > aggregatorsStart && i < aggregatorsEnd }

// isAggregator returns true if the Item is an aggregator that takes a parameter.
// Returns false otherwise
func (i ItemType) IsAggregatorWithParam() bool {
	return i == TOPK || i == BOTTOMK || i == COUNT_VALUES || i == QUANTILE
}

// isKeyword returns true if the Item corresponds to a keyword.
// Returns false otherwise.
func (i ItemType) IsKeyword() bool { return i > keywordsStart && i < keywordsEnd }

// IsComparisonOperator returns true if the Item corresponds to a comparison operator.
// Returns false otherwise.
func (i ItemType) IsComparisonOperator() bool {
	switch i {
	case EQL, NEQ, LTE, LSS, GTE, GTR:
		return true
	default:
		return false
	}
}

// isSetOperator returns whether the Item corresponds to a set operator.
func (i ItemType) IsSetOperator() bool {
	switch i {
	case LAND, LOR, LUNLESS:
		return true
	}
	return false
}

func (i ItemType) String() string {
	if s, ok := ItemTypeStr[i]; ok {
		return s
	}
	return fmt.Sprintf("<Item %d>", i)
}

func (i ItemType) desc() string {
	switch i {
	case ERROR:
		return "error"
	case EOF:
		return "end of input"
	case COMMENT:
		return "comment"
	case IDENTIFIER:
		return "identifier"
	case METRIC_IDENTIFIER:
		return "metric identifier"
	case STRING:
		return "string"
	case NUMBER:
		return "number"
	case DURATION:
		return "duration"
	}
	return fmt.Sprintf("%q", i)
}

// This is a list of all keywords in PromQL.
//
// When changing this list, make sure to also change the maybe_label grammar rule
// in the generated parser to avoid misinterpretation of labels as keywords.
var key = map[string]ItemType{

	// Operators.
	"and":    LAND,
	"or":     LOR,
	"unless": LUNLESS,

	// Aggregators.
	"sum":          SUM,
	"avg":          AVG,
	"count":        COUNT,
	"min":          MIN,
	"max":          MAX,
	"stddev":       STDDEV,
	"stdvar":       STDVAR,
	"topk":         TOPK,
	"bottomk":      BOTTOMK,
	"count_values": COUNT_VALUES,
	"quantile":     QUANTILE,

	// Keywords.
	"offset":      OFFSET,
	"by":          BY,
	"without":     WITHOUT,
	"on":          ON,
	"ignoring":    IGNORING,
	"group_left":  GROUP_LEFT,
	"group_right": GROUP_RIGHT,
	"bool":        BOOL,
}


// ItemTypeStr is the default string representations for common Items.
//
// It does not imply that those are the only character sequences that can be lexed to such an Item.
var ItemTypeStr = map[ItemType]string{

	LEFT_PAREN:    "(",
	RIGHT_PAREN:   ")",
	LEFT_BRACE:    "{",
	RIGHT_BRACE:   "}",
	LEFT_BRACKET:  "[",
	RIGHT_BRACKET: "]",
	COMMA:         ",",
	ASSIGN:        "=",
	COLON:         ":",
	SEMICOLON:     ";",
	BLANK:         "_",
	TIMES:         "x",
	SPACE:         "<space>",

	SUB:       "-",
	ADD:       "+",
	MUL:       "*",
	MOD:       "%",
	DIV:       "/",
	EQL:       "==",
	NEQ:       "!=",
	LTE:       "<=",
	LSS:       "<",
	GTE:       ">=",
	GTR:       ">",
	EQL_REGEX: "=~",
	NEQ_REGEX: "!~",
	POW:       "^",
}



func init() {

	// Add keywords to Item type strings.
	for s, ty := range key {
		ItemTypeStr[ty] = s
	}

	// Special numbers.
	key["inf"] = NUMBER
	key["nan"] = NUMBER
}

const eof = -1

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*Lexer) stateFn

// Pos is the position in a string.
// Negative numbers indicate undefined positions.
type Pos int



// lexer 本质是一个 scan expression => token 的状态机，
//
// lexer 结构体里面定义了和这个状态机有关的状态信息，
// 里面比较有特色的是 state，这里并不向一版的状态机用一个 string 表示状态，而是给了一个下一个 token 的处理函数
// 这样这个state，不仅体现了状态，同时把此状态需要的处理函数也传过来了



// Lexer holds the state of the scanner.
type Lexer struct {

	input       string  // The string being scanned.
	state       stateFn // The next lexing function to enter.
	pos         Pos     // Current position in the input.
	start       Pos     // Start position of this Item.
	width       Pos     // Width of last rune read from input.
	lastPos     Pos     // Position of most recent Item returned by NextItem.
	itemp       *Item   // Pointer to where the next scanned item should be placed.
	scannedItem bool    // Set to true every time an item is scanned.


	parenDepth  int  	// Nesting depth of ( ) exprs.
	braceOpen   bool 	// Whether a { is opened.
	bracketOpen bool 	// Whether a [ is opened.
	gotColon    bool 	// Whether we got a ':' after [ was opened.
	stringOpen  rune 	// Quote rune of the string currently being read.


	// seriesDesc is set when a series description for the testing language is lexed.
	seriesDesc bool
}

// next returns the next rune in the input.
func (l *Lexer) next() rune {

	// 如果当前偏移超过输入字符串总长度，则返回 eof
	if int(l.pos) >= len(l.input) {
		l.width = 0
		return eof
	}

	// 读取一个 rune
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])

	// 保存当前 rune 的字节长度
	l.width = Pos(w)

	// 更新读偏移 pos
	l.pos += l.width

	// 返回 rune
	return r
}

// peek returns but does not consume the next rune in the input.
//
// 预读取一个 rune
func (l *Lexer) peek() rune {
	// 读取 rune
	r := l.next()
	// 回退 rune
	l.backup()
	// 返回 rune
	return r
}

// backup steps back one rune.
// Can only be called once per call of next.
//
// 回退一个 rune
func (l *Lexer) backup() {
	l.pos -= l.width
}

// emit passes an Item back to the client.
//
//
func (l *Lexer) emit(t ItemType) {

	*l.itemp = Item{
		t,
		l.start,
		l.input[l.start:l.pos],
	}

	l.start = l.pos
	l.scannedItem = true
}


// ignore skips over the pending input before this point.
//
//
func (l *Lexer) ignore() {
	l.start = l.pos
}

// accept consumes the next rune if it's from the valid set.
//
//
func (l *Lexer) accept(valid string) bool {

	//
	if strings.ContainsRune(valid, l.next()) {
		return true
	}


	l.backup()

	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *Lexer) acceptRun(valid string) {

	for strings.ContainsRune(valid, l.next()) {
		// consume
	}

	l.backup()
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.NextItem.
func (l *Lexer) errorf(format string, args ...interface{}) stateFn {

	*l.itemp = Item{
		ERROR,
		l.start,
		fmt.Sprintf(format, args...),
	}

	l.scannedItem = true

	return nil
}

// NextItem writes the next item to the provided address.
//
//
func (l *Lexer) NextItem(itemp *Item) {

	l.scannedItem = false
	l.itemp = itemp

	if l.state != nil {
		for !l.scannedItem {
			l.state = l.state(l)
		}
	} else {
		l.emit(EOF)
	}

	l.lastPos = l.itemp.Pos
}

// Lex creates a new scanner for the input string.
func Lex(input string) *Lexer {
	l := &Lexer{
		input: input,
		state: lexStatements,
	}
	return l
}

// lineComment is the character that starts a line comment.
const lineComment = "#"


// lexStatements is the top-level state for lexing.
func lexStatements(l *Lexer) stateFn {

	// "{"
	if l.braceOpen {
		return lexInsideBraces
	}

	// "#"
	if strings.HasPrefix(l.input[l.pos:], lineComment) {
		return lexLineComment
	}


	switch r := l.next(); {
	// 读到字符串尾
	case r == eof:
		// 检查当前嵌套的 ( ) 层数
		if l.parenDepth != 0 {
			return l.errorf("unclosed left parenthesis")
		// 检查当前 "{" 尚未闭合
		} else if l.bracketOpen {
			return l.errorf("unclosed left bracket")
		}
		// 读取 token，类型为 EOF
		l.emit(EOF)
		return nil
	case r == ',':
		l.emit(COMMA)
	case isSpace(r):// 空白字符
		return lexSpace
	case r == '*':
		l.emit(MUL)
	case r == '/':
		l.emit(DIV)
	case r == '%':
		l.emit(MOD)
	case r == '+':
		l.emit(ADD)
	case r == '-':
		l.emit(SUB)
	case r == '^':
		l.emit(POW)
	case r == '=':
		t := l.peek() 	// 预读取 rune
		if t == '=' {
			l.next()	// 读取 rune
			l.emit(EQL)
		} else if t == '~' {
			return l.errorf("unexpected character after '=': %q", t)
		} else {
			l.emit(ASSIGN)
		}
	case r == '!':
		t := l.next() 	// 读取 rune
		if t == '=' {
			l.emit(NEQ)
		} else {
			return l.errorf("unexpected character after '!': %q", t)
		}
	case r == '<':
		if t := l.peek(); t == '=' {
			l.next()
			l.emit(LTE)
		} else {
			l.emit(LSS)
		}
	case r == '>':
		if t := l.peek(); t == '=' {
			l.next()
			l.emit(GTE)
		} else {
			l.emit(GTR)
		}

	//
	case isDigit(r) || (r == '.' && isDigit(l.peek())):
		l.backup()
		return lexNumberOrDuration
	case r == '"' || r == '\'':
		l.stringOpen = r
		return lexString
	case r == '`':
		l.stringOpen = r
		return lexRawString
	case isAlpha(r) || r == ':':
		if !l.bracketOpen {
			l.backup()
			return lexKeywordOrIdentifier
		}
		if l.gotColon {
			return l.errorf("unexpected colon %q", r)
		}
		l.emit(COLON)
		l.gotColon = true
	case r == '(':
		l.emit(LEFT_PAREN)
		l.parenDepth++
		return lexStatements
	case r == ')':
		l.emit(RIGHT_PAREN)
		l.parenDepth--
		if l.parenDepth < 0 {
			return l.errorf("unexpected right parenthesis %q", r)
		}
		return lexStatements
	case r == '{':
		l.emit(LEFT_BRACE)
		l.braceOpen = true
		return lexInsideBraces
	case r == '[':
		if l.bracketOpen {
			return l.errorf("unexpected left bracket %q", r)
		}
		l.gotColon = false
		l.emit(LEFT_BRACKET)
		if isSpace(l.peek()) {
			skipSpaces(l)
		}
		l.bracketOpen = true
		return lexDuration
	case r == ']':
		if !l.bracketOpen {
			return l.errorf("unexpected right bracket %q", r)
		}
		l.emit(RIGHT_BRACKET)
		l.bracketOpen = false

	default:
		return l.errorf("unexpected character: %q", r)
	}

	return lexStatements
}




// lexInsideBraces scans the inside of a vector selector.
// Keywords are ignored and scanned as identifiers.
func lexInsideBraces(l *Lexer) stateFn {

	// 处理注释
	if strings.HasPrefix(l.input[l.pos:], lineComment) {
		return lexLineComment
	}

	// 处理 "{" 中的内容
	switch r := l.next(); {
	case r == eof:
		return l.errorf("unexpected end of input inside braces")
	case isSpace(r):
		return lexSpace
	case isAlpha(r):
		l.backup()
		return lexIdentifier
	case r == ',':
		l.emit(COMMA)
	case r == '"' || r == '\'':
		l.stringOpen = r
		return lexString
	case r == '`':
		l.stringOpen = r
		return lexRawString
	case r == '=':
		if l.next() == '~' {
			l.emit(EQL_REGEX)
			break
		}
		l.backup()
		l.emit(EQL)
	case r == '!':
		switch nr := l.next(); {
		case nr == '~':
			l.emit(NEQ_REGEX)
		case nr == '=':
			l.emit(NEQ)
		default:
			return l.errorf("unexpected character after '!' inside braces: %q", nr)
		}
	case r == '{':
		return l.errorf("unexpected left brace %q", r)
	case r == '}':
		l.emit(RIGHT_BRACE)
		l.braceOpen = false
		if l.seriesDesc {
			return lexValueSequence
		}
		return lexStatements
	default:
		return l.errorf("unexpected character inside braces: %q", r)
	}

	return lexInsideBraces
}


// lexValueSequence scans a value sequence of a series description.
//
//
func lexValueSequence(l *Lexer) stateFn {
	switch r := l.next(); {
	case r == eof:
		return lexStatements
	case isSpace(r):
		l.emit(SPACE)
		lexSpace(l)
	case r == '+':
		l.emit(ADD)
	case r == '-':
		l.emit(SUB)
	case r == 'x':
		l.emit(TIMES)
	case r == '_':
		l.emit(BLANK)
	case isDigit(r) || (r == '.' && isDigit(l.peek())):
		l.backup()
		lexNumber(l)
	case isAlpha(r):
		l.backup()
		// We might lex invalid Items here but this will be caught by the parser.
		return lexKeywordOrIdentifier
	default:
		return l.errorf("unexpected character in series sequence: %q", r)
	}
	return lexValueSequence
}

// lexEscape scans a string escape sequence.
// The initial escaping character (\) has already been seen.
//
// NOTE:
//
// This function as well as the helper function digitVal() and associated
// tests have been adapted from the corresponding functions in the "go/scanner"
// package of the Go standard library to work for Prometheus-style strings.
//
// None of the actual escaping/quoting logic was changed in this function -
// it was only modified to integrate with our lexer.
//
//
// 识别转义字符
func lexEscape(l *Lexer) stateFn {
	var n int
	var base, max uint32

	ch := l.next()
	switch ch {
	case 'a', 'b', 'f', 'n', 'r', 't', 'v', '\\', l.stringOpen:
		return lexString
	case '0', '1', '2', '3', '4', '5', '6', '7':
		n, base, max = 3, 8, 255
	case 'x':
		ch = l.next()
		n, base, max = 2, 16, 255
	case 'u':
		ch = l.next()
		n, base, max = 4, 16, unicode.MaxRune
	case 'U':
		ch = l.next()
		n, base, max = 8, 16, unicode.MaxRune
	case eof:
		l.errorf("escape sequence not terminated")
		return lexString
	default:
		l.errorf("unknown escape sequence %#U", ch)
		return lexString
	}

	var x uint32
	for n > 0 {
		d := uint32(digitVal(ch))
		if d >= base {
			if ch == eof {
				l.errorf("escape sequence not terminated")
				return lexString
			}
			l.errorf("illegal character %#U in escape sequence", ch)
			return lexString
		}
		x = x*base + d
		ch = l.next()
		n--
	}

	if x > max || 0xD800 <= x && x < 0xE000 {
		l.errorf("escape sequence is an invalid Unicode code point")
	}
	return lexString
}

// digitVal returns the digit value of a rune or 16 in case the rune does not
// represent a valid digit.
func digitVal(ch rune) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch - '0')
	case 'a' <= ch && ch <= 'f':
		return int(ch - 'a' + 10)
	case 'A' <= ch && ch <= 'F':
		return int(ch - 'A' + 10)
	}
	return 16 // Larger than any legal digit val.
}

// skipSpaces skips the spaces until a non-space is encountered.
func skipSpaces(l *Lexer) {
	for isSpace(l.peek()) {
		l.next()
	}
	l.ignore()
}

// lexString scans a quoted string. The initial quote has already been seen.
//
// 识别引号中的文本，包括单双引号
func lexString(l *Lexer) stateFn {
Loop:
	for {
		switch l.next() {
		case '\\':
			return lexEscape
		case utf8.RuneError:
			l.errorf("invalid UTF-8 rune")
			return lexString
		case eof, '\n':
			return l.errorf("unterminated quoted string")
		case l.stringOpen:
			break Loop
		}
	}
	l.emit(STRING)
	return lexStatements
}

// lexRawString scans a raw quoted string. The initial quote has already been seen.
//
// 识别原始的文本
func lexRawString(l *Lexer) stateFn {
Loop:
	for {
		switch l.next() {
		case utf8.RuneError:
			l.errorf("invalid UTF-8 rune")
			return lexRawString
		case eof:
			l.errorf("unterminated raw string")
			return lexRawString
		case l.stringOpen:
			break Loop
		}
	}
	l.emit(STRING)
	return lexStatements
}

// lexSpace scans a run of space characters.
// One space has already been seen.
//
// 识别空白字符
func lexSpace(l *Lexer) stateFn {
	for isSpace(l.peek()) {
		l.next()
	}
	l.ignore()
	return lexStatements
}



// lexLineComment scans a line comment.
//
// Left comment marker is known to be present.
//
//
//
func lexLineComment(l *Lexer) stateFn {

	// 跳过 "#"
	l.pos += Pos(len(lineComment))

	// 不断读取 rune ，直到遇到换行符或者字符串结尾
	for r := l.next(); !isEndOfLine(r) && r != eof; {
		r = l.next()
	}

	// 回退一个 rune
	l.backup()

	// 读取注释内容
	l.emit(COMMENT)

	return lexStatements
}


// 识别时间区间 smhdwy
func lexDuration(l *Lexer) stateFn {
	if l.scanNumber() {
		return l.errorf("missing unit character in duration")
	}
	// Next two chars must be a valid unit and a non-alphanumeric.
	if l.accept("smhdwy") {
		if isAlphaNumeric(l.next()) {
			return l.errorf("bad duration syntax: %q", l.input[l.start:l.pos])
		}
		l.backup()
		l.emit(DURATION)
		return lexStatements
	}
	return l.errorf("bad duration syntax: %q", l.input[l.start:l.pos])
}

// lexNumber scans a number: decimal, hex, oct or float.
//
// 识别数字
func lexNumber(l *Lexer) stateFn {
	if !l.scanNumber() {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}
	l.emit(NUMBER)
	return lexStatements
}

// lexNumberOrDuration scans a number or a duration Item.
//
// 识别数组或者时间区间
func lexNumberOrDuration(l *Lexer) stateFn {
	if l.scanNumber() {
		l.emit(NUMBER)
		return lexStatements
	}
	// Next two chars must be a valid unit and a non-alphanumeric.
	if l.accept("smhdwy") {
		if isAlphaNumeric(l.next()) {
			return l.errorf("bad number or duration syntax: %q", l.input[l.start:l.pos])
		}
		l.backup()
		l.emit(DURATION)
		return lexStatements
	}
	return l.errorf("bad number or duration syntax: %q", l.input[l.start:l.pos])
}

// scanNumber scans numbers of different formats.
//
// The scanned Item is not necessarily a valid number.
// This case is caught by the parser.
func (l *Lexer) scanNumber() bool {
	digits := "0123456789"
	// Disallow hexadecimal in series descriptions as the syntax is ambiguous.
	if !l.seriesDesc && l.accept("0") && l.accept("xX") {
		digits = "0123456789abcdefABCDEF"
	}
	l.acceptRun(digits)
	if l.accept(".") {
		l.acceptRun(digits)
	}
	if l.accept("eE") {
		l.accept("+-")
		l.acceptRun("0123456789")
	}
	// Next thing must not be alphanumeric unless it's the times token
	// for series repetitions.
	if r := l.peek(); (l.seriesDesc && r == 'x') || !isAlphaNumeric(r) {
		return true
	}
	return false
}


// lexIdentifier scans an alphanumeric identifier.
//
// The next character is known to be a letter.
//
// 识别字母、数字、标记符号
func lexIdentifier(l *Lexer) stateFn {

	// 不断读取 rune，遇到非字母、非数字的字符时停止。
	for isAlphaNumeric(l.next()) {
		// absorb
	}

	// 回退一个 rune
	l.backup()

	// 取出当前 token ，它是一个标识符
	l.emit(IDENTIFIER)

	//
	return lexStatements
}



// lexKeywordOrIdentifier scans an alphanumeric identifier which may contain a colon rune.
// If the identifier is a keyword the respective keyword Item is scanned.
//
// lexKeywordOrIdentifier 扫描一个可能包含冒号的字母数字标识符。 如果标识符是一个关键字，则会扫描相应的关键字。
//
// 别字母、数字、标记符号（labelname）或关键字。
func lexKeywordOrIdentifier(l *Lexer) stateFn {


Loop:

	for {
		r := l.next() 											// 读取一个 rune
		switch {
		case isAlphaNumeric(r) || r == ':':						// 如果 rune 是字母、数字、":"，则继续读取下个 rune
			// absorb.
		default:												// 否则，意味着读到当前 token 结尾，取出 token

			l.backup() 											// 回退当前 rune
			token := l.input[l.start:l.pos] 					// 取 token
			if kw, ok := key[strings.ToLower(token)]; ok { 		// 检查 token 是否 keyword 关键字
				l.emit(kw)
			} else if !strings.Contains(token, ":") { 	// 检查 token 是否包含 ":"，若包含则为标识符
				l.emit(IDENTIFIER)
			} else {											// 其它，当作 metric标识符
				l.emit(METRIC_IDENTIFIER)
			}
			break Loop
		}
	}

	// 预读一个 rune ，检查其是否为 '{' 。
	if l.seriesDesc && l.peek() != '{' {
		return lexValueSequence
	}

	return lexStatements
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

// isEndOfLine reports whether r is an end-of-line character.
func isEndOfLine(r rune) bool {
	return r == '\r' || r == '\n'
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func isAlphaNumeric(r rune) bool {
	return isAlpha(r) || isDigit(r)
}

// isDigit reports whether r is a digit. Note: we cannot use unicode.IsDigit()
// instead because that also classifies non-Latin digits as digits.
// See https://github.com/blastbao/prometheus/issues/939.
func isDigit(r rune) bool {
	return '0' <= r && r <= '9'
}

// isAlpha reports whether r is an alphabetic or underscore.
func isAlpha(r rune) bool {
	return r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z')
}

// isLabel reports whether the string can be used as label.
func isLabel(s string) bool {

	if len(s) == 0 || !isAlpha(rune(s[0])) {
		return false
	}

	for _, c := range s[1:] {
		if !isAlphaNumeric(c) {
			return false
		}
	}

	return true
}
