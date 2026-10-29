package queueconfig

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseQueueIDs(t *testing.T) {
	tests := []struct {
		in      string
		want    []uint16
		wantErr string // ошибочный элемент (ConfError.Value), "" - без ошибки
	}{
		{in: "", want: []uint16{}},
		{in: ",", want: []uint16{}},
		{in: "1122", want: []uint16{1122}},
		{in: "1122,1123", want: []uint16{1122, 1123}},
		{in: "1122 , 1123 ,,65535", want: []uint16{1122, 1123, 65535}},
		{in: "1122,x", wantErr: "x"},
		{in: "65536", wantErr: "65536"},
		{in: "-1", wantErr: "-1"},
		{in: "1 2", wantErr: "1 2"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseQueueIDs(tt.in)

			if tt.wantErr != "" {
				var confErr *ConfError

				if !errors.As(err, &confErr) {
					t.Fatalf("ParseQueueIDs(%q): err = %v, want *ConfError", tt.in, err)
				}

				if confErr.Path != "queue_ids" || confErr.Value != tt.wantErr {
					t.Errorf("ParseQueueIDs(%q): ConfError = %+v, want Path queue_ids, Value %q", tt.in, confErr, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("ParseQueueIDs(%q): unexpected error %v", tt.in, err)
			}

			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("ParseQueueIDs(%q) mismatch (-want +got):\n%s", tt.in, diff)
			}
		})
	}
}
