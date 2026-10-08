package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

type mismatch struct {
	Kind    string
	Context string // "" or "." means the default context
	Subject string
	Version int // 0 when not version-specific
	Detail  string
}

func (m mismatch) String() string {
	ctxLabel := ""
	if m.Context != "" && m.Context != "." {
		ctxLabel = fmt.Sprintf(" context=%q", m.Context)
	}
	switch {
	case m.Version != 0:
		return fmt.Sprintf("[%s]%s subject=%q version=%d: %s", m.Kind, ctxLabel, m.Subject, m.Version, m.Detail)
	case m.Subject != "":
		return fmt.Sprintf("[%s]%s subject=%q: %s", m.Kind, ctxLabel, m.Subject, m.Detail)
	default:
		return fmt.Sprintf("[%s]%s %s", m.Kind, ctxLabel, m.Detail)
	}
}

type report struct {
	mu               sync.Mutex
	mismatches       []mismatch
	subjectsA        int
	subjectsB        int
	versionsChecked  int
	schemaIDsChecked int
	contextsChecked  int
}

func (r *report) add(m mismatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mismatches = append(r.mismatches, m)
}

func (r *report) incVersionsChecked() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.versionsChecked++
}

func (r *report) incSchemaIDsChecked() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemaIDsChecked++
}

func (r *report) addSubjectCounts(a, b int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subjectsA += a
	r.subjectsB += b
}

func (r *report) incContextsChecked() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contextsChecked++
}

func (r *report) snapshot() []mismatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]mismatch, len(r.mismatches))
	copy(out, r.mismatches)
	return out
}

// idCollector accumulates schema IDs seen while walking subjects/versions,
// for an independent follow-up check against the global /schemas/ids/{id}
// lookup path.
type idCollector struct {
	mu  sync.Mutex
	ids map[int]bool
}

func newIDCollector() *idCollector {
	return &idCollector{ids: map[int]bool{}}
}

func (c *idCollector) add(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids[id] = true
}

func (c *idCollector) list() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, 0, len(c.ids))
	for id := range c.ids {
		out = append(out, id)
	}
	sort.Ints(out)
	return out
}

func stringSetDiff(a, b []string) (onlyA, onlyB, both []string) {
	setA := make(map[string]bool, len(a))
	for _, s := range a {
		setA[s] = true
	}
	setB := make(map[string]bool, len(b))
	for _, s := range b {
		setB[s] = true
	}
	for _, s := range a {
		if setB[s] {
			both = append(both, s)
		} else {
			onlyA = append(onlyA, s)
		}
	}
	for _, s := range b {
		if !setA[s] {
			onlyB = append(onlyB, s)
		}
	}
	sort.Strings(onlyA)
	sort.Strings(onlyB)
	sort.Strings(both)
	return
}

func intSetDiff(a, b []int) (onlyA, onlyB, both []int) {
	setA := make(map[int]bool, len(a))
	for _, v := range a {
		setA[v] = true
	}
	setB := make(map[int]bool, len(b))
	for _, v := range b {
		setB[v] = true
	}
	for _, v := range a {
		if setB[v] {
			both = append(both, v)
		} else {
			onlyA = append(onlyA, v)
		}
	}
	for _, v := range b {
		if !setA[v] {
			onlyB = append(onlyB, v)
		}
	}
	sort.Ints(onlyA)
	sort.Ints(onlyB)
	sort.Ints(both)
	return
}

// schemasEqual compares two schema strings: first by exact byte equality,
// then - if that fails - by parsing both as JSON and deep-comparing, so a
// purely cosmetic formatting difference is distinguished from a real
// content difference in the report.
func schemasEqual(a, b string) (equal, cosmeticOnly bool) {
	if a == b {
		return true, false
	}
	var ja, jb any
	if json.Unmarshal([]byte(a), &ja) != nil {
		return false, false
	}
	if json.Unmarshal([]byte(b), &jb) != nil {
		return false, false
	}
	if reflect.DeepEqual(ja, jb) {
		return false, true
	}
	return false, false
}

// jsonRawEqual compares two arbitrary JSON values (e.g. a schema's
// Data Contracts "metadata"/"ruleSet" fields) by parsed, semantic equality
// rather than raw bytes - field order within a JSON object is never
// meaningful. Both absent (nil/empty) compares equal.
func jsonRawEqual(a, b json.RawMessage) bool {
	var ja, jb any
	if len(a) > 0 {
		if err := json.Unmarshal(a, &ja); err != nil {
			ja = string(a)
		}
	}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &jb); err != nil {
			jb = string(b)
		}
	}
	return reflect.DeepEqual(ja, jb)
}

func referencesEqual(a, b []reference) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
