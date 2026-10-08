package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// client is a minimal Confluent-compatible Schema Registry API client.
// pathPrefix is empty for the default context, or "/contexts/{name}" when
// scoped to a non-default Schema Registry context. user/pass, when set,
// are sent as HTTP Basic Auth on every request.
type client struct {
	base       string
	hc         *http.Client
	pathPrefix string
	user, pass string
}

func newClient(base string, hc *http.Client, user, pass string) *client {
	return &client{base: base, hc: hc, user: user, pass: pass}
}

// withPathPrefix returns a client scoped to the given context path prefix,
// sharing the same base URL, HTTP client, and credentials.
func (c *client) withPathPrefix(prefix string) *client {
	return &client{base: c.base, hc: c.hc, pathPrefix: prefix, user: c.user, pass: c.pass}
}

// contextPrefix maps a context name to its URL path prefix. The default
// context ("" or ".") needs no prefix at all.
func contextPrefix(name string) string {
	if name == "" || name == "." {
		return ""
	}
	return "/contexts/" + url.PathEscape(name)
}

// get issues a GET request, retrying a small number of times on a
// connection-level failure (e.g. "resource temporarily unavailable" from a
// kubectl port-forward tunnel or ephemeral-port pressure under high local
// concurrency). HTTP-level responses (any status code) are never retried.
func (c *client) get(ctx context.Context, path string, out any) (int, error) {
	const maxAttempts = 5
	backoff := 100 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		status, err := c.doGet(ctx, path, out)
		if err == nil {
			return status, nil
		}
		lastErr = err
		if attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return 0, lastErr
}

func (c *client) doGet(ctx context.Context, path string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return 0, err
	}
	if c.user != "" || c.pass != "" {
		req.SetBasicAuth(c.user, c.pass)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK && out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return resp.StatusCode, nil
}

// discoverContexts calls GET /contexts on a root (unscoped) client. If the
// endpoint isn't supported (older SR, or Redpanda build without it), it
// falls back to just the default context and reports supported=false.
func discoverContexts(ctx context.Context, c *client) (names []string, supported bool) {
	var ctxs []string
	status, err := c.get(ctx, "/contexts", &ctxs)
	if err != nil || status != http.StatusOK || len(ctxs) == 0 {
		return []string{"."}, false
	}
	sort.Strings(ctxs)
	return ctxs, true
}

// subjects returns every subject, including soft-deleted ones, sorted.
func (c *client) subjects(ctx context.Context) ([]string, error) {
	var subs []string
	status, err := c.get(ctx, c.pathPrefix+"/subjects?deleted=true", &subs)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s/subjects: status %d", c.pathPrefix, status)
	}
	sort.Strings(subs)
	return subs, nil
}

// versions returns every version of a subject, including soft-deleted ones, sorted.
func (c *client) versions(ctx context.Context, subject string) ([]int, error) {
	var vers []int
	path := c.pathPrefix + "/subjects/" + url.PathEscape(subject) + "/versions?deleted=true"
	status, err := c.get(ctx, path, &vers)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", path, status)
	}
	sort.Ints(vers)
	return vers, nil
}

type reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type schemaVersion struct {
	Subject    string          `json:"subject"`
	ID         int             `json:"id"`
	Version    int             `json:"version"`
	Schema     string          `json:"schema"`
	SchemaType string          `json:"schemaType,omitempty"`
	References []reference     `json:"references,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	RuleSet    json.RawMessage `json:"ruleSet,omitempty"`
}

func (c *client) version(ctx context.Context, subject string, version int) (*schemaVersion, error) {
	var sv schemaVersion
	path := fmt.Sprintf("%s/subjects/%s/versions/%d?deleted=true", c.pathPrefix, url.PathEscape(subject), version)
	status, err := c.get(ctx, path, &sv)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", path, status)
	}
	return &sv, nil
}

// latestVersion returns (version, found, error). found=false with err=nil
// means the subject has no resolvable "latest" version (SR returns 404 -
// notably the normal case for a soft-deleted subject, since "latest" does
// not accept ?deleted=true).
func (c *client) latestVersion(ctx context.Context, subject string) (*schemaVersion, bool, error) {
	var sv schemaVersion
	path := c.pathPrefix + "/subjects/" + url.PathEscape(subject) + "/versions/latest"
	status, err := c.get(ctx, path, &sv)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("GET %s: status %d", path, status)
	}
	return &sv, true, nil
}

type schemaByID struct {
	Schema     string          `json:"schema"`
	SchemaType string          `json:"schemaType,omitempty"`
	References []reference     `json:"references,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	RuleSet    json.RawMessage `json:"ruleSet,omitempty"`
}

// byID returns (schema, found, error) for the global schema-ID index -
// a lookup path independent of any subject/version index. found=false
// with err=nil means the ID isn't known to this registry (404).
func (c *client) byID(ctx context.Context, id int) (*schemaByID, bool, error) {
	var s schemaByID
	path := fmt.Sprintf("%s/schemas/ids/%d", c.pathPrefix, id)
	status, err := c.get(ctx, path, &s)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("GET %s: status %d", path, status)
	}
	return &s, true, nil
}

type srConfig struct {
	CompatibilityLevel string          `json:"compatibilityLevel"`
	DefaultMetadata    json.RawMessage `json:"defaultMetadata,omitempty"`
	OverrideMetadata   json.RawMessage `json:"overrideMetadata,omitempty"`
	DefaultRuleSet     json.RawMessage `json:"defaultRuleSet,omitempty"`
	OverrideRuleSet    json.RawMessage `json:"overrideRuleSet,omitempty"`
}

func (c *client) globalConfig(ctx context.Context) (*srConfig, error) {
	var cfg srConfig
	status, err := c.get(ctx, c.pathPrefix+"/config", &cfg)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s/config: status %d", c.pathPrefix, status)
	}
	return &cfg, nil
}

// subjectConfig returns (config, found, error). found=false with err=nil
// means the subject has no compatibility override (SR returns 404).
func (c *client) subjectConfig(ctx context.Context, subject string) (*srConfig, bool, error) {
	var cfg srConfig
	path := c.pathPrefix + "/config/" + url.PathEscape(subject)
	status, err := c.get(ctx, path, &cfg)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("GET %s: status %d", path, status)
	}
	return &cfg, true, nil
}

type srMode struct {
	Mode string `json:"mode"`
}

func (c *client) globalMode(ctx context.Context) (*srMode, error) {
	var m srMode
	status, err := c.get(ctx, c.pathPrefix+"/mode", &m)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s/mode: status %d", c.pathPrefix, status)
	}
	return &m, nil
}

// subjectMode returns (mode, found, error). found=false with err=nil means
// the subject has no mode override (SR returns 404).
func (c *client) subjectMode(ctx context.Context, subject string) (*srMode, bool, error) {
	var m srMode
	path := c.pathPrefix + "/mode/" + url.PathEscape(subject)
	status, err := c.get(ctx, path, &m)
	if err != nil {
		return nil, false, err
	}
	if status == http.StatusNotFound {
		return nil, false, nil
	}
	if status != http.StatusOK {
		return nil, false, fmt.Errorf("GET %s: status %d", path, status)
	}
	return &m, true, nil
}
