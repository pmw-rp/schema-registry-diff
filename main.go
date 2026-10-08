// sr-compare validates that two Confluent-compatible Schema Registry
// instances hold identical data: subjects, versions, schema content/type,
// references, schema IDs (via both the subject/version index and the
// global ID index), "latest" resolution, per-subject and global config,
// per-subject and global mode, and (when supported) contexts.
//
// Usage:
//
//	sr-compare -a http://localhost:28081 -b http://localhost:38081
//	sr-compare -a https://sr-a:8081 -b https://sr-b:8081 \
//	  -tls-ca ca.pem -a-user u1 -a-pass p1 -b-user u2 -b-pass p2
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

func main() {
	var (
		urlA        = flag.String("a", "", "base URL of Schema Registry A (baseline), e.g. http://localhost:28081")
		urlB        = flag.String("b", "", "base URL of Schema Registry B (comparison), e.g. http://localhost:38081")
		concurrency = flag.Int("concurrency", 8, "number of subjects (or schema IDs) compared concurrently; keep modest against a kubectl port-forward tunnel")
		timeout     = flag.Duration("timeout", 30*time.Second, "per-request HTTP timeout")
		verbose     = flag.Bool("v", false, "print progress while checking subjects")

		tlsCA         = flag.String("tls-ca", "", "path to a CA certificate (PEM) to trust, in addition to system roots (applies to both -a and -b)")
		tlsCert       = flag.String("tls-cert", "", "path to a client certificate (PEM), for mTLS (applies to both -a and -b)")
		tlsKey        = flag.String("tls-key", "", "path to a client private key (PEM), for mTLS (applies to both -a and -b)")
		tlsSkipVerify = flag.Bool("tls-insecure-skip-verify", false, "skip TLS certificate verification (test/dev only)")

		userA = flag.String("a-user", "", "HTTP Basic Auth username for A")
		passA = flag.String("a-pass", "", "HTTP Basic Auth password for A")
		userB = flag.String("b-user", "", "HTTP Basic Auth username for B")
		passB = flag.String("b-pass", "", "HTTP Basic Auth password for B")
	)
	flag.Parse()

	if *urlA == "" || *urlB == "" {
		fmt.Fprintln(os.Stderr, "usage: sr-compare -a <base-url-A> -b <base-url-B> [-concurrency N] [-v]")
		os.Exit(2)
	}

	transport := &http.Transport{
		MaxIdleConns:        4 * (*concurrency),
		MaxIdleConnsPerHost: 4 * (*concurrency),
		IdleConnTimeout:     90 * time.Second,
	}
	if *tlsCA != "" || *tlsCert != "" || *tlsKey != "" || *tlsSkipVerify {
		tlsCfg, err := buildTLSConfig(*tlsCA, *tlsCert, *tlsKey, *tlsSkipVerify)
		if err != nil {
			fmt.Fprintf(os.Stderr, "tls: %v\n", err)
			os.Exit(2)
		}
		transport.TLSClientConfig = tlsCfg
	}
	hc := &http.Client{Timeout: *timeout, Transport: transport}
	rootA := newClient(*urlA, hc, *userA, *passA)
	rootB := newClient(*urlB, hc, *userB, *passB)
	ctx := context.Background()

	rep := &report{}

	fmt.Printf("A = %s\nB = %s\n\n", *urlA, *urlB)

	ctxsA, supportedA := discoverContexts(ctx, rootA)
	ctxsB, supportedB := discoverContexts(ctx, rootB)
	if supportedA || supportedB {
		fmt.Printf("Contexts: A=%v (GET /contexts supported=%v)  B=%v (supported=%v)\n\n", ctxsA, supportedA, ctxsB, supportedB)
	} else {
		fmt.Println("Contexts: GET /contexts not supported on either side; comparing default context only.")
	}

	ctxOnlyA, ctxOnlyB, ctxBoth := stringSetDiff(ctxsA, ctxsB)
	for _, c := range ctxOnlyA {
		rep.add(mismatch{Kind: "missing-context-in-b", Context: c})
	}
	for _, c := range ctxOnlyB {
		rep.add(mismatch{Kind: "missing-context-in-a", Context: c})
	}

	totalSubjectsCompared := 0
	for _, srCtxName := range ctxBoth {
		prefix := contextPrefix(srCtxName)
		ca := rootA.withPathPrefix(prefix)
		cb := rootB.withPathPrefix(prefix)
		if *verbose {
			fmt.Printf("--- context %q ---\n", srCtxName)
		}
		totalSubjectsCompared += compareContext(ctx, ca, cb, srCtxName, rep, *concurrency, *verbose)
		rep.incContextsChecked()
	}

	printReport(rep, totalSubjectsCompared)

	if len(rep.snapshot()) > 0 {
		os.Exit(1)
	}
}

// compareContext runs the full comparison (global config/mode, subjects,
// versions, per-subject config/mode, latest resolution, and the global
// schema-ID index) for one Schema Registry context. Returns the number of
// subjects present on both sides that were compared.
func compareContext(ctx context.Context, ca, cb *client, srCtxName string, rep *report, concurrency int, verbose bool) int {
	compareGlobalConfig(ctx, ca, cb, srCtxName, rep)
	compareGlobalMode(ctx, ca, cb, srCtxName, rep)

	subsA, err := ca.subjects(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "context %q: fetching subjects from A: %v\n", srCtxName, err)
		return 0
	}
	subsB, err := cb.subjects(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "context %q: fetching subjects from B: %v\n", srCtxName, err)
		return 0
	}
	rep.addSubjectCounts(len(subsA), len(subsB))

	onlyA, onlyB, both := stringSetDiff(subsA, subsB)
	for _, s := range onlyA {
		rep.add(mismatch{Kind: "missing-subject-in-b", Context: srCtxName, Subject: s})
	}
	for _, s := range onlyB {
		rep.add(mismatch{Kind: "missing-subject-in-a", Context: srCtxName, Subject: s})
	}

	ids := newIDCollector()

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var checkedMu sync.Mutex
	checked := 0

	for _, subject := range both {
		wg.Add(1)
		sem <- struct{}{}
		go func(subject string) {
			defer wg.Done()
			defer func() { <-sem }()
			compareSubject(ctx, ca, cb, srCtxName, subject, rep, ids)
			if verbose {
				checkedMu.Lock()
				checked++
				n := checked
				checkedMu.Unlock()
				if n%100 == 0 || n == len(both) {
					fmt.Printf("  ...checked %d/%d subjects\n", n, len(both))
				}
			}
		}(subject)
	}
	wg.Wait()

	compareSchemaIDs(ctx, ca, cb, srCtxName, ids.list(), rep, concurrency)

	return len(both)
}

func compareGlobalConfig(ctx context.Context, ca, cb *client, srCtxName string, rep *report) {
	cfgA, errA := ca.globalConfig(ctx)
	cfgB, errB := cb.globalConfig(ctx)
	if errA != nil || errB != nil {
		fmt.Fprintf(os.Stderr, "warning: context %q: could not fetch global config (A err=%v, B err=%v)\n", srCtxName, errA, errB)
		return
	}
	if cfgA.CompatibilityLevel != cfgB.CompatibilityLevel {
		rep.add(mismatch{Kind: "global-config", Context: srCtxName, Detail: fmt.Sprintf("A=%q B=%q", cfgA.CompatibilityLevel, cfgB.CompatibilityLevel)})
	}
	compareConfigMetadata(srCtxName, "", "global-config", cfgA, cfgB, rep)
}

// compareConfigMetadata checks the Data Contracts metadata/ruleSet fields
// that can appear on a /config (or /config/{subject}) response, shared by
// the global and per-subject config comparisons.
func compareConfigMetadata(srCtxName, subject, kindPrefix string, a, b *srConfig, rep *report) {
	if !jsonRawEqual(a.DefaultMetadata, b.DefaultMetadata) {
		rep.add(mismatch{Kind: kindPrefix + "-default-metadata-mismatch", Context: srCtxName, Subject: subject, Detail: "defaultMetadata differs"})
	}
	if !jsonRawEqual(a.OverrideMetadata, b.OverrideMetadata) {
		rep.add(mismatch{Kind: kindPrefix + "-override-metadata-mismatch", Context: srCtxName, Subject: subject, Detail: "overrideMetadata differs"})
	}
	if !jsonRawEqual(a.DefaultRuleSet, b.DefaultRuleSet) {
		rep.add(mismatch{Kind: kindPrefix + "-default-ruleset-mismatch", Context: srCtxName, Subject: subject, Detail: "defaultRuleSet differs"})
	}
	if !jsonRawEqual(a.OverrideRuleSet, b.OverrideRuleSet) {
		rep.add(mismatch{Kind: kindPrefix + "-override-ruleset-mismatch", Context: srCtxName, Subject: subject, Detail: "overrideRuleSet differs"})
	}
}

func compareGlobalMode(ctx context.Context, ca, cb *client, srCtxName string, rep *report) {
	modeA, errA := ca.globalMode(ctx)
	modeB, errB := cb.globalMode(ctx)
	if errA != nil || errB != nil {
		fmt.Fprintf(os.Stderr, "warning: context %q: could not fetch global mode (A err=%v, B err=%v)\n", srCtxName, errA, errB)
		return
	}
	if modeA.Mode != modeB.Mode {
		rep.add(mismatch{Kind: "global-mode", Context: srCtxName, Detail: fmt.Sprintf("A=%q B=%q", modeA.Mode, modeB.Mode)})
	}
}

func compareSubject(ctx context.Context, ca, cb *client, srCtxName, subject string, rep *report, ids *idCollector) {
	cfgA, foundA, errA := ca.subjectConfig(ctx, subject)
	cfgB, foundB, errB := cb.subjectConfig(ctx, subject)
	if errA == nil && errB == nil {
		if foundA != foundB {
			rep.add(mismatch{Kind: "subject-config-presence", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("foundA=%v foundB=%v", foundA, foundB)})
		} else if foundA && foundB {
			if cfgA.CompatibilityLevel != cfgB.CompatibilityLevel {
				rep.add(mismatch{Kind: "subject-config-mismatch", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("A=%q B=%q", cfgA.CompatibilityLevel, cfgB.CompatibilityLevel)})
			}
			compareConfigMetadata(srCtxName, subject, "subject-config", cfgA, cfgB, rep)
		}
	}

	mdA, mfoundA, merrA := ca.subjectMode(ctx, subject)
	mdB, mfoundB, merrB := cb.subjectMode(ctx, subject)
	if merrA == nil && merrB == nil {
		if mfoundA != mfoundB {
			rep.add(mismatch{Kind: "subject-mode-presence", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("foundA=%v foundB=%v", mfoundA, mfoundB)})
		} else if mfoundA && mfoundB && mdA.Mode != mdB.Mode {
			rep.add(mismatch{Kind: "subject-mode-mismatch", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("A=%q B=%q", mdA.Mode, mdB.Mode)})
		}
	}

	compareLatest(ctx, ca, cb, srCtxName, subject, rep)

	versA, err := ca.versions(ctx, subject)
	if err != nil {
		rep.add(mismatch{Kind: "error", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("fetching versions from A: %v", err)})
		return
	}
	versB, err := cb.versions(ctx, subject)
	if err != nil {
		rep.add(mismatch{Kind: "error", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("fetching versions from B: %v", err)})
		return
	}

	vOnlyA, vOnlyB, vBoth := intSetDiff(versA, versB)
	for _, v := range vOnlyA {
		rep.add(mismatch{Kind: "missing-version-in-b", Context: srCtxName, Subject: subject, Version: v})
	}
	for _, v := range vOnlyB {
		rep.add(mismatch{Kind: "missing-version-in-a", Context: srCtxName, Subject: subject, Version: v})
	}

	for _, v := range vBoth {
		rep.incVersionsChecked()

		svA, err := ca.version(ctx, subject, v)
		if err != nil {
			rep.add(mismatch{Kind: "error", Context: srCtxName, Subject: subject, Version: v, Detail: fmt.Sprintf("fetching from A: %v", err)})
			continue
		}
		svB, err := cb.version(ctx, subject, v)
		if err != nil {
			rep.add(mismatch{Kind: "error", Context: srCtxName, Subject: subject, Version: v, Detail: fmt.Sprintf("fetching from B: %v", err)})
			continue
		}

		ids.add(svA.ID)
		ids.add(svB.ID)

		if svA.ID != svB.ID {
			rep.add(mismatch{Kind: "id-mismatch", Context: srCtxName, Subject: subject, Version: v, Detail: fmt.Sprintf("A=%d B=%d", svA.ID, svB.ID)})
		}

		typeA, typeB := svA.SchemaType, svB.SchemaType
		if typeA == "" {
			typeA = "AVRO"
		}
		if typeB == "" {
			typeB = "AVRO"
		}
		if typeA != typeB {
			rep.add(mismatch{Kind: "schematype-mismatch", Context: srCtxName, Subject: subject, Version: v, Detail: fmt.Sprintf("A=%q B=%q", typeA, typeB)})
		}

		if equal, cosmetic := schemasEqual(svA.Schema, svB.Schema); !equal {
			kind, detail := "schema-mismatch", "schema content differs"
			if cosmetic {
				kind, detail = "schema-formatting-difference", "schema is semantically equal but differs byte-for-byte"
			}
			rep.add(mismatch{Kind: kind, Context: srCtxName, Subject: subject, Version: v, Detail: detail})
		}

		if !referencesEqual(svA.References, svB.References) {
			rep.add(mismatch{Kind: "references-mismatch", Context: srCtxName, Subject: subject, Version: v, Detail: fmt.Sprintf("A=%v B=%v", svA.References, svB.References)})
		}

		if !jsonRawEqual(svA.Metadata, svB.Metadata) {
			rep.add(mismatch{Kind: "metadata-mismatch", Context: srCtxName, Subject: subject, Version: v, Detail: "schema metadata differs"})
		}
		if !jsonRawEqual(svA.RuleSet, svB.RuleSet) {
			rep.add(mismatch{Kind: "ruleset-mismatch", Context: srCtxName, Subject: subject, Version: v, Detail: "schema ruleSet differs"})
		}
	}
}

// compareLatest checks GET .../versions/latest directly, since "latest"
// resolution (especially around soft-deleted subjects) is a distinct code
// path from listing versions and fetching each one individually.
func compareLatest(ctx context.Context, ca, cb *client, srCtxName, subject string, rep *report) {
	latA, foundA, errA := ca.latestVersion(ctx, subject)
	latB, foundB, errB := cb.latestVersion(ctx, subject)
	if errA != nil || errB != nil {
		rep.add(mismatch{Kind: "error", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("fetching latest: A err=%v B err=%v", errA, errB)})
		return
	}
	if foundA != foundB {
		rep.add(mismatch{Kind: "latest-presence", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("foundA=%v foundB=%v", foundA, foundB)})
		return
	}
	if !foundA {
		return
	}
	if latA.Version != latB.Version {
		rep.add(mismatch{Kind: "latest-version-mismatch", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("A=v%d B=v%d", latA.Version, latB.Version)})
	}
	if latA.ID != latB.ID {
		rep.add(mismatch{Kind: "latest-id-mismatch", Context: srCtxName, Subject: subject, Detail: fmt.Sprintf("A=%d B=%d", latA.ID, latB.ID)})
	}
	if equal, cosmetic := schemasEqual(latA.Schema, latB.Schema); !equal {
		kind := "latest-schema-mismatch"
		if cosmetic {
			kind = "latest-schema-formatting-difference"
		}
		rep.add(mismatch{Kind: kind, Context: srCtxName, Subject: subject, Detail: "schema content differs at /versions/latest"})
	}
	if !jsonRawEqual(latA.Metadata, latB.Metadata) {
		rep.add(mismatch{Kind: "latest-metadata-mismatch", Context: srCtxName, Subject: subject, Detail: "schema metadata differs at /versions/latest"})
	}
	if !jsonRawEqual(latA.RuleSet, latB.RuleSet) {
		rep.add(mismatch{Kind: "latest-ruleset-mismatch", Context: srCtxName, Subject: subject, Detail: "schema ruleSet differs at /versions/latest"})
	}
}

// compareSchemaIDs independently verifies the global schema-ID index
// (GET /schemas/ids/{id}) for every ID seen while walking subjects and
// versions - a different lookup path through Schema Registry's storage
// than the subject/version index already checked above.
func compareSchemaIDs(ctx context.Context, ca, cb *client, srCtxName string, ids []int, rep *report, concurrency int) {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()
			rep.incSchemaIDsChecked()

			sa, foundA, errA := ca.byID(ctx, id)
			sb, foundB, errB := cb.byID(ctx, id)
			if errA != nil || errB != nil {
				rep.add(mismatch{Kind: "error", Context: srCtxName, Detail: fmt.Sprintf("schema id %d: A err=%v B err=%v", id, errA, errB)})
				return
			}
			if foundA != foundB {
				rep.add(mismatch{Kind: "schema-id-presence", Context: srCtxName, Detail: fmt.Sprintf("id=%d foundA=%v foundB=%v", id, foundA, foundB)})
				return
			}
			if !foundA {
				return
			}

			typeA, typeB := sa.SchemaType, sb.SchemaType
			if typeA == "" {
				typeA = "AVRO"
			}
			if typeB == "" {
				typeB = "AVRO"
			}
			if typeA != typeB {
				rep.add(mismatch{Kind: "schema-id-type-mismatch", Context: srCtxName, Detail: fmt.Sprintf("id=%d A=%q B=%q", id, typeA, typeB)})
			}
			if equal, cosmetic := schemasEqual(sa.Schema, sb.Schema); !equal {
				kind := "schema-id-content-mismatch"
				if cosmetic {
					kind = "schema-id-formatting-difference"
				}
				rep.add(mismatch{Kind: kind, Context: srCtxName, Detail: fmt.Sprintf("id=%d", id)})
			}
			if !referencesEqual(sa.References, sb.References) {
				rep.add(mismatch{Kind: "schema-id-references-mismatch", Context: srCtxName, Detail: fmt.Sprintf("id=%d", id)})
			}
			if !jsonRawEqual(sa.Metadata, sb.Metadata) {
				rep.add(mismatch{Kind: "schema-id-metadata-mismatch", Context: srCtxName, Detail: fmt.Sprintf("id=%d", id)})
			}
			if !jsonRawEqual(sa.RuleSet, sb.RuleSet) {
				rep.add(mismatch{Kind: "schema-id-ruleset-mismatch", Context: srCtxName, Detail: fmt.Sprintf("id=%d", id)})
			}
		}(id)
	}
	wg.Wait()
}

func printReport(rep *report, subjectsCompared int) {
	mismatches := rep.snapshot()

	fmt.Println()
	fmt.Printf("Contexts checked: %d\n", rep.contextsChecked)
	fmt.Printf("Subjects: A=%d B=%d (compared=%d)\n", rep.subjectsA, rep.subjectsB, subjectsCompared)
	fmt.Printf("Versions checked: %d\n", rep.versionsChecked)
	fmt.Printf("Distinct schema IDs checked (global index): %d\n", rep.schemaIDsChecked)
	fmt.Printf("Mismatches: %d\n", len(mismatches))

	if len(mismatches) == 0 {
		fmt.Println("\nRESULT: IDENTICAL")
		return
	}

	fmt.Println("\nRESULT: DIFFERENCES FOUND")

	byKind := map[string]int{}
	for _, m := range mismatches {
		byKind[m.Kind]++
	}
	kinds := make([]string, 0, len(byKind))
	for k := range byKind {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	fmt.Println("\nBy kind:")
	for _, k := range kinds {
		fmt.Printf("  %-36s %d\n", k, byKind[k])
	}

	const limit = 200
	fmt.Println("\nDetails:")
	for i, m := range mismatches {
		if i >= limit {
			fmt.Printf("  ... and %d more\n", len(mismatches)-limit)
			break
		}
		fmt.Println("  " + m.String())
	}
}
