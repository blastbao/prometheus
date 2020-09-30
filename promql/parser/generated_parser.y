// Copyright 2019 The Prometheus Authors
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


// %{ ... }% 之间的代码会直接输出到 go 代码中

%{
package parser

import (
        "math"
        "sort"
        "strconv"
        "time"

        "github.com/blastbao/prometheus/pkg/labels"
        "github.com/blastbao/prometheus/pkg/value"
)
%}


// 以 ％union 开始的行定义了值的类型

%union {
    node      Node
    item      Item                  // 终结符
    matchers  []*labels.Matcher
    matcher   *labels.Matcher
    label     labels.Label
    labels    labels.Labels
    strings   []string
    series    []SequenceValue
    uint      uint64
    float     float64
    duration  time.Duration
}

// 以 ％token 开始的行定义的是终结符，都是大写的，可见 ASSIGN, BLANK 和 COLON 等都是终结符，
//
// 如 ASSIGN, BLANK 和 COLON 这几个终结符的值将是 item 类型。

%token <item>
ASSIGN
BLANK
COLON
COMMA
COMMENT
DURATION
EOF
ERROR
IDENTIFIER
LEFT_BRACE
LEFT_BRACKET
LEFT_PAREN
METRIC_IDENTIFIER
NUMBER
RIGHT_BRACE
RIGHT_BRACKET
RIGHT_PAREN
SEMICOLON
SPACE
STRING
TIMES

// Operators.
%token	operatorsStart
%token <item>
ADD
DIV
EQL
EQL_REGEX
GTE
GTR
LAND
LOR
LSS
LTE
LUNLESS
MOD
MUL
NEQ
NEQ_REGEX
POW
SUB
%token	operatorsEnd

// Aggregators.
%token	aggregatorsStart
%token <item>
AVG
BOTTOMK
COUNT
COUNT_VALUES
MAX
MIN
QUANTILE
STDDEV
STDVAR
SUM
TOPK
%token	aggregatorsEnd

// Keywords.
%token	keywordsStart
%token <item>
BOOL
BY
GROUP_LEFT
GROUP_RIGHT
IGNORING
OFFSET
ON
WITHOUT
%token keywordsEnd


// Start symbols for the generated parser.
%token startSymbolsStart
%token
START_METRIC
START_SERIES_DESCRIPTION
START_EXPRESSION
START_METRIC_SELECTOR
%token	startSymbolsEnd


// 以 ％type 开始的行是说明非终结符的类型定义。
//
// 如非终结符 label_match_list 的值将是 matchers 类型，label_matcher 则对应 matcher 类型。


// Type definitions for grammar rules.
%type <matchers>
label_match_list

%type <matcher>
label_matcher

%type <item>
aggregate_op
grouping_label
match_op
maybe_label
metric_identifier
unary_op

%type <labels>
label_set
label_set_list
metric

%type <label>
label_set_item

%type <strings>
grouping_label_list
grouping_labels
maybe_grouping_labels

%type <series>
series_item
series_values

%type <uint>
uint

%type <float>
number
series_value
signed_number

%type <node>
aggregate_expr
aggregate_modifier
bin_modifier
binary_expr
bool_modifier
expr function_call
function_call_args
function_call_body
group_modifiers
label_matchers
matrix_selector
number_literal
offset_expr
on_or_ignoring
paren_expr
string_literal
subquery_expr
unary_expr
vector_selector

%type <duration>
duration
maybe_duration




// 上下文无关文法的开始符号是一个特殊的非终结符，所有的推导都从这个非终结符开始，在yacc中，语法开始符定义语句是：
// %start 非终结符
// 如果没有上面的说明，yacc 自动将语法规则部分中第一条语法规则左部的非终结符作为语法开始符。

%start start

// ％left/％right/％nonassoc 开头的行就是定义算符的结合性的行。
//
// ％left 表示其后的算符是遵循左结合的；％right 表示右结合性，而 ％nonassoc 则表示其后的算符没有结合性。
// 优先级是隐含的，排在前面行的较后面行的优先级低；排在同一行的优先级相同。

// Operators are listed with increasing precedence.
%left LOR
%left LAND LUNLESS
%left EQL GTE GTR LSS LTE NEQ
%left ADD SUB
%left MUL DIV MOD
%right POW
// Offset modifiers do not have associativity.
%nonassoc OFFSET
// This ensures that it is always attempted to parse range or subquery selectors when a left bracket is encountered.
%right LEFT_BRACKET





%%







start           : START_METRIC metric
                    {
                        yylex.(*parser).generatedParserResult = $2
                    }
                | START_SERIES_DESCRIPTION series_description
                | START_EXPRESSION /* empty */ EOF
                    {
                        yylex.(*parser).addParseErrf(PositionRange{}, "no expression found in input")
                    }
                | START_EXPRESSION expr
                    {
                        yylex.(*parser).generatedParserResult = $2
                    }
                | START_METRIC_SELECTOR vector_selector
                    {
                        yylex.(*parser).generatedParserResult = $2
                    }
                | start EOF
                | error
                    /* If none of the more detailed error messages are triggered, we fall back to this. */
                    {
                        yylex.(*parser).unexpected("","")
                    }
                ;


expr            :
                aggregate_expr      // 聚合表达式
                | binary_expr       // 二元表达式
                | function_call     // 函数调用
                | matrix_selector   // 选择
                | number_literal    //
                | offset_expr       // 偏移量表达式
                | paren_expr        //
                | string_literal    //
                | subquery_expr     // 子查询表达式
                | unary_expr        // 一元表达式
                | vector_selector   //
                ;

/*
 * Aggregations.
 */


// aggregate_op 是 keyword ，终结符
//
// function_call_body 是规则，非终结符



aggregate_expr  : aggregate_op aggregate_modifier function_call_body
                    {

                        // $1: 聚合操作符
                        // $2: 聚合修饰符
                        // $3: 函数参数
                        $$ = yylex.(*parser).newAggregateExpr($1, $2, $3)

                    }
                | aggregate_op function_call_body aggregate_modifier
                    {
                        $$ = yylex.(*parser).newAggregateExpr($1, $3, $2)
                    }
                | aggregate_op function_call_body
                    {
                        $$ = yylex.(*parser).newAggregateExpr($1, &AggregateExpr{}, $2)
                    }
                | aggregate_op error
                    {
                        yylex.(*parser).unexpected("aggregation",""); 
                        $$ = yylex.(*parser).newAggregateExpr($1, &AggregateExpr{}, Expressions{})
                    }
                ;

aggregate_modifier: BY grouping_labels
                        {
                            $$ = &AggregateExpr{
                                Grouping: $2,
                            }
                        }
                    | WITHOUT grouping_labels
                        {
                            $$ = &AggregateExpr{
                                Grouping: $2,
                                Without:  true,
                            }
                        }
                    ;

/*
 * Binary expressions.
 */

// Operator precedence only works if each of those is listed separately.
binary_expr     : expr ADD     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr DIV     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr EQL     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr GTE     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr GTR     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LAND    bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LOR     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LSS     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LTE     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LUNLESS bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr MOD     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr MUL     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr NEQ     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr POW     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr SUB     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                ;

// Using left recursion for the modifier rules, helps to keep the parser stack small and reduces allocations
bin_modifier    : group_modifiers;

bool_modifier   : /* empty */
                    {
                        $$ = &BinaryExpr{
                            VectorMatching: &VectorMatching{Card: CardOneToOne},
                        }
                    }
                | BOOL
                    {
                        $$ = &BinaryExpr{
                            VectorMatching: &VectorMatching{Card: CardOneToOne},
                            ReturnBool: true,
                        }
                    }
                ;

on_or_ignoring  : bool_modifier IGNORING grouping_labels
                    {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.MatchingLabels = $3
                    }
                | bool_modifier ON grouping_labels
                    {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.MatchingLabels = $3
                        $$.(*BinaryExpr).VectorMatching.On = true
                    }
                ;

group_modifiers : bool_modifier /* empty */
                | on_or_ignoring /* empty */
                | on_or_ignoring GROUP_LEFT maybe_grouping_labels
                    {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.Card = CardManyToOne
                        $$.(*BinaryExpr).VectorMatching.Include = $3
                    }
                | on_or_ignoring GROUP_RIGHT maybe_grouping_labels
                    {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.Card = CardOneToMany
                        $$.(*BinaryExpr).VectorMatching.Include = $3
                    }
                ;

grouping_labels : LEFT_PAREN grouping_label_list RIGHT_PAREN
                    {
                        $$ = $2
                    }
                | LEFT_PAREN grouping_label_list COMMA RIGHT_PAREN
                    {
                        $$ = $2
                    }
                | LEFT_PAREN RIGHT_PAREN
                    {
                        $$ = []string{}
                    }
                | error
                    {
                        yylex.(*parser).unexpected("grouping opts", "\"(\""); $$ = nil
                    }
                ;

grouping_label_list : grouping_label_list COMMA grouping_label
                        {
                            $$ = append($1, $3.Val)
                        }
                    | grouping_label
                        {
                            $$ = []string{$1.Val}
                        }
                    | grouping_label_list error
                        {
                            yylex.(*parser).unexpected("grouping opts", "\",\" or \")\"");
                            $$ = $1
                        }
                    ;

grouping_label  : maybe_label
                    {
                        if !isLabel($1.Val) { // 检查是否合法 label 名称
                            yylex.(*parser).unexpected("grouping opts", "label")
                        }
                        $$ = $1
                    }
                | error
                    {
                        yylex.(*parser).unexpected("grouping opts", "label");
                        $$ = Item{}
                    }
                ;

/*
 * Function calls.
 */

function_call   : IDENTIFIER function_call_body
                   {
                        fn, exist := getFunction($1.Val)

                        if !exist{
                            yylex.(*parser).addParseErrf($1.PositionRange(),"unknown function with name %q", $1.Val)
                        }

                        $$ = &Call{
                            Func: fn,
                            Args: $2.(Expressions),
                            PosRange: PositionRange{
                                Start: $1.Pos,
                                End: yylex.(*parser).lastClosing,
                            },
                        }
                    }
                ;


function_call_body  : LEFT_PAREN function_call_args RIGHT_PAREN
                        {
                            $$ = $2
                        }
                    | LEFT_PAREN RIGHT_PAREN
                        {
                            $$ = Expressions{}
                        }
                    ;

function_call_args  : function_call_args COMMA expr
                        {
                            $$ = append($1.(Expressions), $3.(Expr))
                        }
                    | expr
                        {
                            $$ = Expressions{$1.(Expr)}
                        }
                    | function_call_args COMMA
                        {
                            yylex.(*parser).addParseErrf($2.PositionRange(), "trailing commas not allowed in function call args")
                            $$ = $1
                        }
                    ;

/*
 * Expressions inside parentheses.
 */

paren_expr      : LEFT_PAREN expr RIGHT_PAREN
                    {
                        $$ = &ParenExpr{
                            Expr: $2.(Expr),
                            PosRange: mergeRanges(&$1, &$3),
                        }
                    }
                ;

/*
 * Offset modifiers.
 */

offset_expr: expr OFFSET duration
                    {
                        yylex.(*parser).addOffset($1, $3)
                        $$ = $1
                    }
                | expr OFFSET error
                    {
                        yylex.(*parser).unexpected("offset", "duration");
                        $$ = $1
                    }
                ;

/*
 * Subquery and range selectors.
 */

matrix_selector : expr LEFT_BRACKET duration RIGHT_BRACKET
                    {
                        var errMsg string
                        vs, ok := $1.(*VectorSelector)
                        if !ok{
                            errMsg = "ranges only allowed for vector selectors"
                        } else if vs.Offset != 0{
                            errMsg = "no offset modifiers allowed before range"
                        }

                        if errMsg != ""{
                            errRange := mergeRanges(&$2, &$4)
                            yylex.(*parser).addParseErrf(errRange, errMsg)
                        }

                        $$ = &MatrixSelector{
                            VectorSelector: $1.(Expr),
                            Range: $3,
                            EndPos: yylex.(*parser).lastClosing,
                        }
                    }
                ;

subquery_expr   : expr LEFT_BRACKET duration COLON maybe_duration RIGHT_BRACKET
                    {
                        $$ = &SubqueryExpr{
                                Expr:  $1.(Expr),
                                Range: $3,
                                Step:  $5,

                                EndPos: $6.Pos + 1,
                        }
                    }
                | expr LEFT_BRACKET duration COLON duration error
                    {
                        yylex.(*parser).unexpected("subquery selector", "\"]\"");
                        $$ = $1
                    }
                | expr LEFT_BRACKET duration COLON error
                    {
                        yylex.(*parser).unexpected("subquery selector", "duration or \"]\"");
                        $$ = $1
                    }
                | expr LEFT_BRACKET duration error
                    {
                        yylex.(*parser).unexpected("subquery or range", "\":\" or \"]\"");
                        $$ = $1
                    }
                | expr LEFT_BRACKET error
                    {
                        yylex.(*parser).unexpected("subquery selector", "duration");
                        $$ = $1
                    }
                ;

/*
 * Unary expressions.
 */

unary_expr      :
                /* gives the rule the same precedence as MUL. This aligns with mathematical conventions */
                unary_op expr %prec MUL
                    {
                        if nl, ok := $2.(*NumberLiteral); ok {
                            if $1.Typ == SUB {
                                nl.Val *= -1
                            }
                            nl.PosRange.Start = $1.Pos
                            $$ = nl
                        } else {
                            $$ = &UnaryExpr{Op: $1.Typ, Expr: $2.(Expr), StartPos: $1.Pos}
                        }
                    }
                ;

/*
 * Vector selectors.
 */


vector_selector: metric_identifier label_matchers
                    {
                        vs := $2.(*VectorSelector)
                        vs.PosRange = mergeRanges(&$1, vs)
                        vs.Name = $1.Val
                        yylex.(*parser).assembleVectorSelector(vs)
                        $$ = vs
                    }
                | metric_identifier
                    {
                        vs := &VectorSelector{
                                Name: $1.Val,
                                LabelMatchers: []*labels.Matcher{},
                                PosRange: $1.PositionRange(),
                        }
                        yylex.(*parser).assembleVectorSelector(vs)
                        $$ = vs
                    }
                | label_matchers
                    {
                        vs := $1.(*VectorSelector)
                        yylex.(*parser).assembleVectorSelector(vs)
                        $$ = vs
                    }
                ;

label_matchers  : LEFT_BRACE label_match_list RIGHT_BRACE
                    {
                        $$ = &VectorSelector{
                                LabelMatchers: $2,
                                PosRange: mergeRanges(&$1, &$3),
                        }
                    }
                | LEFT_BRACE label_match_list COMMA RIGHT_BRACE
                    {
                        $$ = &VectorSelector{
                                LabelMatchers: $2,
                                PosRange: mergeRanges(&$1, &$4),
                        }
                    }
                | LEFT_BRACE RIGHT_BRACE
                    {
                        $$ = &VectorSelector{
                                LabelMatchers: []*labels.Matcher{},
                                PosRange: mergeRanges(&$1, &$2),
                        }
                    }
                ;


label_match_list: label_match_list COMMA label_matcher
                    {
                        if $1 != nil{
                            $$ = append($1, $3)
                        } else {
                            $$ = $1
                        }
                    }
                | label_matcher
                    {
                        $$ = []*labels.Matcher{$1}
                    }
                | label_match_list error
                    {
                        yylex.(*parser).unexpected("label matching", "\",\" or \"}\"");
                        $$ = $1
                    }
                ;


// $1 表示右边的第一个标记的值
// $2 表示右边的第二个标记的值，依次类推。
// $$ 表示规约后的值，因前面定义了 "%type <matcher> label_matcher"，所以 label_matcher 规约后的值会自动填充到 union.matcher 字段上，并返回 union 对象。

// error 表示 yacc 在分析 label_matcher 推导出的句型时，遇到语法错误时跳过出错的部分，继续分析（也会打印语法错误信息）。

label_matcher   : IDENTIFIER match_op STRING
                    {
                        // 构造标签匹配的 matcher
                        $$ = yylex.(*parser).newLabelMatcher($1, $2, $3);
                    }
                | IDENTIFIER match_op error
                    {
                        yylex.(*parser).unexpected("label matching", "string");
                        $$ = nil
                    }
                | IDENTIFIER error
                    {
                        yylex.(*parser).unexpected("label matching", "label matching operator");
                        $$ = nil
                    }
                | error
                    {
                        yylex.(*parser).unexpected("label matching", "identifier or \"}\"");
                        $$ = nil
                    }
                ;




/*
 * Metric descriptions.
 */


//
//
//
// 前面定义了 "%type <labels> metric" ，所以规约后的值会自动设置到 yyVAL.labels 字段上。


metric          : metric_identifier label_set
                    {
                        $$ = append($2, labels.Label{ Name: labels.MetricName, Value: $1.Val });
                        sort.Sort($$)
                    }
                | label_set
                    {
                        $$ = $1
                    }
                ;

metric_identifier   : AVG
                    | BOTTOMK
                    | BY
                    | COUNT
                    | COUNT_VALUES
                    | IDENTIFIER
                    | LAND
                    | LOR
                    | LUNLESS
                    | MAX
                    | METRIC_IDENTIFIER
                    | MIN
                    | OFFSET
                    | QUANTILE
                    | STDDEV
                    | STDVAR
                    | SUM
                    | TOPK;


// (label list)
label_set       : LEFT_BRACE label_set_list RIGHT_BRACE
                    {
                        $$ = labels.New($2...)
                    }
                | LEFT_BRACE label_set_list COMMA RIGHT_BRACE
                    {
                        $$ = labels.New($2...)
                    }
                | LEFT_BRACE RIGHT_BRACE
                    {
                        $$ = labels.New()
                    }
                | /* empty */
                    {
                        $$ = labels.New()
                    }
                ;

// label list
label_set_list  : label_set_list COMMA label_set_item
                    {
                        $$ = append($1, $3)
                    }
                | label_set_item
                    {
                        $$ = []labels.Label{$1}
                    }
                | label_set_list error
                    {
                        yylex.(*parser).unexpected("label set", "\",\" or \"}\"", );
                        $$ = $1
                    }
                ;

// label
label_set_item  : IDENTIFIER EQL STRING
                    {
                        $$ = labels.Label{
                            Name: $1.Val,
                            Value: yylex.(*parser).unquoteString($3.Val),
                        }
                    }
                | IDENTIFIER EQL error
                    {
                        yylex.(*parser).unexpected("label set", "string");
                        $$ = labels.Label{}
                    }
                | IDENTIFIER error
                    {
                        yylex.(*parser).unexpected("label set", "\"=\"");
                        $$ = labels.Label{}
                    }
                | error
                    {
                        yylex.(*parser).unexpected("label set", "identifier or \"}\"");
                        $$ = labels.Label{}
                    }
                ;

/*
 * Series descriptions (only used by unit tests).
 */

series_description  : metric series_values
                        {
                            yylex.(*parser).generatedParserResult = &seriesDescription{
                                labels: $1,
                                values: $2,
                            }
                        }
                    ;


series_values   : /*empty*/
                    {
                        $$ = []SequenceValue{}
                    }
                | series_values SPACE series_item
                    {
                        $$ = append($1, $3...)
                    }
                | series_values SPACE
                    {
                        $$ = $1
                    }
                | error
                    {
                        yylex.(*parser).unexpected("series values", "");
                        $$ = nil
                    }
                ;


series_item     : BLANK
                    {
                        $$ = []SequenceValue{ {Omitted: true} }
                    }
                | BLANK TIMES uint
                    {
                        $$ = []SequenceValue{}
                        for i:=uint64(0); i < $3; i++{
                            $$ = append($$, SequenceValue{Omitted: true})
                        }
                    }
                | series_value
                    {
                        $$ = []SequenceValue{{Value: $1}}
                    }
                | series_value TIMES uint
                    {
                        $$ = []SequenceValue{}
                        for i:=uint64(0); i <= $3; i++{
                            $$ = append($$, SequenceValue{Value: $1})
                        }
                    }
                | series_value signed_number TIMES uint
                    {
                        $$ = []SequenceValue{}
                        for i:=uint64(0); i <= $4; i++{
                            $$ = append($$, SequenceValue{Value: $1})
                            $1 += $2
                        }
                    }
                ;


series_value    : IDENTIFIER
                    {
                        if $1.Val != "stale" {
                                yylex.(*parser).unexpected("series values", "number or \"stale\"")
                        }
                        $$ = math.Float64frombits(value.StaleNaN)
                    }
                | number
                | signed_number
                ;




/*
 * Keyword lists.
 */

// 关键字也是非终结符

aggregate_op    : AVG
                | BOTTOMK
                | COUNT
                | COUNT_VALUES
                | MAX
                | MIN
                | QUANTILE
                | STDDEV
                | STDVAR
                | SUM
                | TOPK ;

// inside of grouping options label names can be recognized as keywords by the lexer.
// This is a list of keywords that could also be a label name.

maybe_label     : AVG
                | BOOL
                | BOTTOMK
                | BY
                | COUNT
                | COUNT_VALUES
                | GROUP_LEFT
                | GROUP_RIGHT
                | IDENTIFIER
                | IGNORING
                | LAND
                | LOR
                | LUNLESS
                | MAX
                | METRIC_IDENTIFIER
                | MIN
                | OFFSET
                | ON
                | QUANTILE
                | STDDEV
                | STDVAR
                | SUM
                | TOPK;


unary_op        : ADD
                | SUB;

match_op        : EQL
                | NEQ
                | EQL_REGEX
                | NEQ_REGEX ;



/*
 * Literals.
 */

number_literal  : NUMBER
                    {
                        $$ = &NumberLiteral{
                            Val: yylex.(*parser).number($1.Val),
                            PosRange: $1.PositionRange(),
                        }
                    }
                ;

// 数字

number          : NUMBER
                    {
                        $$ = yylex.(*parser).number($1.Val)
                    }
                ;



// 有符号数

signed_number   : ADD number
                    {
                        $$ = $2
                    }
                | SUB number
                    {
                        $$ = -$2
                    }
                ;


// $1 代表右边第一个符号的值，由于 NUMBER 对应的类型为 Item ， 所以取值方式为 $1.Val 。
// $$ 代表当前规则处理之后，左边符号的值。

uint            : NUMBER
                    {
                        var err error
                        $$, err = strconv.ParseUint($1.Val, 10, 64) // 将字符串转换成整数
                        if err != nil {
                            yylex.(*parser).addParseErrf($1.PositionRange(), "invalid repetition in series values: %s", err)
                        }
                    }
                ;


// 时间

duration        : DURATION
                    {
                        var err error
                        $$, err = parseDuration($1.Val) // parses a string into a time.Duration
                        if err != nil {
                            yylex.(*parser).addParseErr($1.PositionRange(), err)
                        }
                    }
                ;

// 字符串

string_literal  : STRING
                    {
                        $$ = &StringLiteral{
                            Val: yylex.(*parser).unquoteString($1.Val),
                            PosRange: $1.PositionRange(),
                        }
                    }
                ;

/*
 * Wrappers for optional arguments.
 */

maybe_duration  : /* empty */
                    {
                        $$ = 0
                    }
                | duration
                ;

maybe_grouping_labels   : /* empty */
                            { $$ = nil }
                        | grouping_labels
                        ;

%%
