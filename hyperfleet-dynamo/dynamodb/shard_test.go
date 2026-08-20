package dynamodb

import (
	"testing"
)

func TestComputeShard_Deterministic(t *testing.T) {
	id := "a3f1b2c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c"
	first := ComputeShardDefault(id)
	for i := 0; i < 100; i++ {
		if got := ComputeShardDefault(id); got != first {
			t.Fatalf("ComputeShardDefault(%q) = %q on iteration %d, want %q", id, got, i, first)
		}
	}
}

func TestComputeShard_AllBucketsReachable(t *testing.T) {
	seen := make(map[string]bool)
	// Use a range of UUIDs that covers all 4 buckets.
	uuids := []string{
		"00000000-0000-0000-0000-000000000000",
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
		"44444444-4444-4444-4444-444444444444",
		"55555555-5555-5555-5555-555555555555",
		"66666666-6666-6666-6666-666666666666",
		"77777777-7777-7777-7777-777777777777",
		"88888888-8888-8888-8888-888888888888",
		"99999999-9999-9999-9999-999999999999",
		"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
	}
	for _, id := range uuids {
		seen[ComputeShardDefault(id)] = true
	}
	for _, bucket := range []string{"0", "1", "2", "3"} {
		if !seen[bucket] {
			t.Errorf("bucket %q never produced by ComputeShardDefault across test IDs", bucket)
		}
	}
}

func TestComputeShard_MatchesPythonAlgorithm(t *testing.T) {
	// Values verified against create_table_and_load.py:compute_shard.
	// compute_shard strips hyphens, takes first 8 hex chars, parses as int, mod 4.
	cases := []struct {
		id   string
		want string
	}{
		// "a3f1b2c4" = 0xa3f1b2c4 = 2751644356; 2751644356 % 4 = 0
		{"a3f1b2c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c", "0"},
		// "00000001" = 1; 1 % 4 = 1
		{"00000001-0000-0000-0000-000000000000", "1"},
		// "00000002" = 2; 2 % 4 = 2
		{"00000002-0000-0000-0000-000000000000", "2"},
		// "00000003" = 3; 3 % 4 = 3
		{"00000003-0000-0000-0000-000000000000", "3"},
		// "00000004" = 4; 4 % 4 = 0
		{"00000004-0000-0000-0000-000000000000", "0"},
	}
	for _, tc := range cases {
		got := ComputeShardDefault(tc.id)
		if got != tc.want {
			t.Errorf("ComputeShardDefault(%q) = %q, want %q", tc.id, got, tc.want)
		}
	}
}

func TestComputeShard_ShortID(t *testing.T) {
	// IDs shorter than 8 hex chars after stripping hyphens should not panic.
	got := ComputeShard("abc", 4)
	if got != "0" {
		t.Errorf("ComputeShard(short) = %q, want \"0\"", got)
	}
}
