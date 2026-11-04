package safepath

import "testing"

func TestPaths(t *testing.T) {
	for _, p := range []string{"../x", "/etc/passwd", "a/../../x", "C:/x", "a\\x", "a:stream", "CON.txt", "a/..", "a/", "a.", ""} {
		if Relative(p) == nil {
			t.Errorf("accepted %q", p)
		}
	}
	for _, p := range []string{".", "a/b.txt", "hello world", "data.bin"} {
		if e := Relative(p); e != nil {
			t.Errorf("%s: %v", p, e)
		}
	}
}
