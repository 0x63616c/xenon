// Package visibility implements Xenon's visibility value semantics.
package visibility

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

type textPosition struct{ pos, weight int }
type textVector map[string][]textPosition
type textQuery struct {
	op          byte
	word        string
	weights     int
	prefix      bool
	distance    int
	left, right *textQuery
}
type textParser struct {
	s        string
	i, nodes int
}

func textSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
func (p *textParser) space() {
	for p.i < len(p.s) && textSpace(p.s[p.i]) {
		p.i++
	}
}
func (p *textParser) word(query bool) (string, error) {
	p.space()
	if p.i == len(p.s) {
		return "", fmt.Errorf("missing text lexeme")
	}
	var b strings.Builder
	quoted := p.s[p.i] == '\''
	if quoted {
		p.i++
	}
	for p.i < len(p.s) {
		c := p.s[p.i]
		if c == '\\' {
			p.i++
			if p.i == len(p.s) {
				return "", fmt.Errorf("missing escaped character")
			}
			_, n := utf8.DecodeRuneInString(p.s[p.i:])
			b.WriteString(p.s[p.i : p.i+n])
			p.i += n
			continue
		}
		if quoted {
			if c == '\'' {
				p.i++
				if p.i < len(p.s) && p.s[p.i] == '\'' {
					b.WriteByte('\'')
					p.i++
					continue
				}
				if b.Len() == 0 {
					return "", fmt.Errorf("empty lexeme")
				}
				return b.String(), nil
			}
		} else if textSpace(c) || (c == ':' && (query || b.Len() > 0)) || (query && strings.ContainsRune("!&|()<", rune(c))) {
			break
		}
		b.WriteByte(c)
		p.i++
	}
	if quoted {
		return "", fmt.Errorf("unterminated lexeme")
	}
	if b.Len() == 0 {
		return "", fmt.Errorf("empty lexeme")
	}
	if b.Len() >= 2047 {
		return "", fmt.Errorf("lexeme too long")
	}
	return b.String(), nil
}
func parseTextVector(s string) (textVector, error) {
	p := textParser{s: s}
	v := textVector{}
	for {
		p.space()
		if p.i == len(s) {
			return v, nil
		}
		w, e := p.word(false)
		if e != nil {
			return nil, e
		}
		if len(w) >= 2047 {
			return nil, fmt.Errorf("lexeme too long")
		}
		positions := []textPosition{}
		if p.i < len(s) && s[p.i] == ':' {
			p.i++
			for {
				start := p.i
				for p.i < len(s) && s[p.i] >= '0' && s[p.i] <= '9' {
					p.i++
				}
				if start == p.i {
					return nil, fmt.Errorf("invalid vector position")
				}
				parsed, _ := strconv.ParseInt(s[start:p.i], 10, 64)
				n := int32(parsed)
				if n == 0 {
					return nil, fmt.Errorf("zero vector position")
				}
				if n > 16383 {
					n = 16383
				}
				n &= 16383
				if n == 0 {
					return nil, fmt.Errorf("zero vector position")
				}
				weight := 0
				for p.i < len(s) && !textSpace(s[p.i]) && s[p.i] != ',' {
					c := s[p.i]
					if c >= '0' && c <= '9' {
						p.i++
						continue
					}
					nw := -1
					switch c {
					case 'A', 'a', '*':
						nw = 3
					case 'B', 'b':
						nw = 2
					case 'C', 'c':
						nw = 1
					case 'D', 'd':
						nw = 0
					}
					if nw < 0 || weight != 0 {
						return nil, fmt.Errorf("invalid vector weight")
					}
					weight = nw
					p.i++
				}
				positions = append(positions, textPosition{int(n), weight})
				if p.i == len(s) || s[p.i] != ',' {
					break
				}
				p.i++
			}
		}
		old, exists := v[w]
		if !exists {
			v[w] = positions
		} else {
			v[w] = append(old, positions...)
		}
		all := v[w]
		sort.Slice(all, func(i, j int) bool {
			if all[i].pos == all[j].pos {
				return all[i].weight > all[j].weight
			}
			return all[i].pos < all[j].pos
		})
		unique := all[:0]
		for _, x := range all {
			if len(unique) == 0 || unique[len(unique)-1].pos != x.pos {
				unique = append(unique, x)
			}
		}
		if len(unique) > 256 {
			unique = unique[:256]
		}
		v[w] = unique
	}
}
func (p *textParser) expr(min int) (*textQuery, error) {
	p.space()
	if p.i == len(p.s) {
		return nil, fmt.Errorf("missing query operand")
	}
	p.nodes++
	if p.nodes > 1024 {
		return nil, fmt.Errorf("text query nesting limit")
	}
	var a *textQuery
	switch p.s[p.i] {
	case '!':
		p.i++
		child, e := p.expr(4)
		if e != nil {
			return nil, e
		}
		a = &textQuery{op: '!', left: child}
	case '(':
		p.i++
		child, e := p.expr(1)
		if e != nil {
			return nil, e
		}
		p.space()
		if p.i == len(p.s) || p.s[p.i] != ')' {
			return nil, fmt.Errorf("unclosed query group")
		}
		p.i++
		a = child
	default:
		w, e := p.word(true)
		if e != nil {
			return nil, e
		}
		if len(w) >= 2047 {
			return nil, fmt.Errorf("lexeme too long")
		}
		a = &textQuery{word: w}
		if p.i < len(p.s) && p.s[p.i] == ':' {
			p.i++
			for p.i < len(p.s) {
				c := p.s[p.i]
				if c == '*' {
					a.prefix = true
					p.i++
					continue
				}
				weight := strings.IndexByte("DCBA", byte(strings.ToUpper(string(c))[0]))
				if weight < 0 {
					break
				}
				a.weights |= 1 << weight
				p.i++
			}
		}
	}
	for {
		p.space()
		if p.i == len(p.s) || p.s[p.i] == ')' {
			return a, nil
		}
		op := p.s[p.i]
		prec := 0
		switch op {
		case '|':
			prec = 1
		case '&':
			prec = 2
		case '<':
			prec = 3
		}
		if prec < min || prec == 0 {
			return a, nil
		}
		p.i++
		dist := 0
		if op == '<' {
			if p.i+1 < len(p.s) && p.s[p.i:p.i+2] == "->" {
				dist = 1
				p.i += 2
			} else {
				start := p.i
				for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
					p.i++
				}
				if start == p.i || p.i == len(p.s) || p.s[p.i] != '>' {
					return nil, fmt.Errorf("invalid phrase distance")
				}
				n, e := strconv.Atoi(p.s[start:p.i])
				if e != nil || n > 16384 {
					return nil, fmt.Errorf("invalid phrase distance")
				}
				dist = n
				p.i++
			}
		}
		right, e := p.expr(prec + 1)
		if e != nil {
			return nil, e
		}
		a = &textQuery{op: op, left: a, right: right, distance: dist}
	}
}

// A positional match is a finite set or its complement, aligned at phrase ends.
// maybe means the vector has a lexeme without positions. PostgreSQL resolves
// that uncertainty to false at the outermost phrase operator, before Boolean NOT.
type textMatches struct {
	positions     map[int]bool
	negate, maybe bool
	width         int
}

func (m textMatches) yes() bool { return m.negate || len(m.positions) > 0 }
func textOperand(v textVector, q *textQuery, position bool) textMatches {
	r := textMatches{positions: map[int]bool{}}
	for word, ps := range v {
		if word != q.word && !(q.prefix && strings.HasPrefix(word, q.word)) {
			continue
		}
		if len(ps) == 0 {
			if position {
				r.maybe = true
			} else {
				r.negate = true
			}
			continue
		}
		for _, x := range ps {
			if q.weights == 0 || q.weights&(1<<x.weight) != 0 {
				r.positions[x.pos] = true
			}
		}
	}
	return r
}
func textPhrase(v textVector, q *textQuery) textMatches {
	if q.op == 0 {
		return textOperand(v, q, true)
	}
	l := textPhrase(v, q.left)
	if q.op == '!' {
		if !l.maybe {
			l.negate = !l.negate
		}
		return l
	}
	r := textPhrase(v, q.right)
	and := q.op != '|'
	if and && ((!l.yes() && !l.maybe) || (!r.yes() && !r.maybe)) {
		return textMatches{}
	}
	if !and && !l.yes() && !l.maybe && !r.yes() && !r.maybe {
		return textMatches{}
	}
	if l.maybe || r.maybe {
		return textMatches{maybe: true}
	}
	if !l.yes() {
		l.width = 0
	}
	if !r.yes() {
		r.width = 0
	}
	width := max(l.width, r.width)
	lo, ro := width-l.width, width-r.width
	if q.op == '<' {
		lo = q.distance + r.width
		ro = 0
		width = q.distance + l.width + r.width
	}
	lp, rp := map[int]bool{}, map[int]bool{}
	for x := range l.positions {
		lp[(x&16383)+lo] = true
	}
	for x := range r.positions {
		rp[(x&16383)+ro] = true
	}
	result := textMatches{positions: map[int]bool{}, width: width}
	if and {
		result.negate = l.negate && r.negate
	} else {
		result.negate = l.negate || r.negate
	}
	candidates := map[int]bool{}
	for x := range lp {
		candidates[x] = true
	}
	for x := range rp {
		candidates[x] = true
	}
	for x := range candidates {
		left := lp[x] != l.negate
		right := rp[x] != r.negate
		match := left && right
		if !and {
			match = left || right
		}
		if match != result.negate && x > 0 {
			result.positions[x&65535] = true
		}
	}
	return result
}
func textExecute(v textVector, q *textQuery) bool {
	switch q.op {
	case 0:
		return textOperand(v, q, false).yes()
	case '!':
		return !textExecute(v, q.left)
	case '&':
		return textExecute(v, q.left) && textExecute(v, q.right)
	case '|':
		return textExecute(v, q.left) || textExecute(v, q.right)
	default:
		m := textPhrase(v, q)
		return !m.maybe && m.yes()
	}
}

// TextMatch evaluates the pinned Temporal PostgreSQL Text policy: the stored
// string is tsvector input, and ASCII-space-separated query pieces are joined
// with OR before parsing tsquery. There is no stemming or case folding.
func TextMatch(vector, query string) (bool, error) {
	if !utf8.ValidString(vector) || !utf8.ValidString(query) || strings.IndexByte(vector, 0) >= 0 || strings.IndexByte(query, 0) >= 0 {
		return false, fmt.Errorf("invalid text encoding")
	}
	v, e := parseTextVector(vector)
	if e != nil {
		return false, e
	}
	parts := strings.Split(query, " ")
	tokens := parts[:0]
	for _, part := range parts {
		if part != "" {
			tokens = append(tokens, part)
		}
	}
	p := textParser{s: strings.Join(tokens, " | ")}
	p.space()
	if p.i == len(p.s) {
		return false, nil
	}
	q, e := p.expr(1)
	if e != nil {
		return false, e
	}
	p.space()
	if p.i != len(p.s) {
		return false, fmt.Errorf("unexpected query token")
	}
	return textExecute(v, q), nil
}
