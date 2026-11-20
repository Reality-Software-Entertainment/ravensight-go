package ravensight

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// testdata/scrub-vectors.json is a byte-for-byte copy of the platform's
// shared scrub cases. Every Ravensight SDK and the server must produce `out`
// for each `in`.
func TestScrubVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/scrub-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Version int `json:"version"`
		Cases   []struct {
			Name string `json:"name"`
			In   string `json:"in"`
			Out  string `json:"out"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if file.Version != 6 || len(file.Cases) == 0 {
		t.Fatalf("unexpected vectors: version %d, %d cases", file.Version, len(file.Cases))
	}
	for _, tc := range file.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			if got := scrub(tc.In); got != tc.Out {
				t.Errorf("scrub(%q)\n got %q\nwant %q", tc.In, got, tc.Out)
			}
			if again := scrub(tc.Out); again != tc.Out {
				t.Errorf("scrub is not idempotent: %q became %q", tc.Out, again)
			}
		})
	}
}

func TestScrubFieldCutsBeforeAndAfter(t *testing.T) {
	long := strings.Repeat("a", 5000) + " jane@example.com"
	if got := scrubField(long, 200); utf16Len(got) != 200 {
		t.Fatalf("got %d units", utf16Len(got))
	}
	if got := scrubField("mail jane@example.com", 200); got != "mail [email]" {
		t.Fatalf("got %q", got)
	}
	// A huge input is cut before the scrub runs.
	if got := scrubField(strings.Repeat("x", 1<<20), 2000); len(got) != 2000 {
		t.Fatalf("got %d", len(got))
	}
}

func TestCutUnitsNeverSplitsACharacter(t *testing.T) {
	text := strings.Repeat("😀", 10) // 20 units
	if got := cutUnits(text, 5); got != strings.Repeat("😀", 2) {
		t.Fatalf("got %q", got)
	}
	if got := cutUnits("éé", 1); got != "é" {
		t.Fatalf("got %q", got)
	}
	if utf16Len("a😀é") != 4 {
		t.Fatal("utf16Len")
	}
}

func TestMessageTemplate(t *testing.T) {
	a := messageTemplate(`player 42 missing item "sword" at https://x.io/a?b=1 id 3f2a9c1e-7b4d-4e8a-9c3f-1a2b3c4d5e6f`)
	b := messageTemplate(`player 7 missing item "shield" at https://y.io/c id 0f2a9c1e-7b4d-4e8a-9c3f-1a2b3c4d5e6f`)
	if a != b {
		t.Fatalf("templates differ:\n%s\n%s", a, b)
	}
}
