package queueconfig

import "testing"

func TestParseSize(t *testing.T) {
	ok := []struct {
		in   string
		want uint64
	}{
		// голые числа - байты (всё, что принимает strconv.ParseFloat)
		{"0", 0},
		{"10", 10},
		{"1.5", 1},       // усечение до целых байт
		{"1e9", 1e9},     // экспоненциальная запись
		{"0x1p10", 1024}, // hex float - тоже принимался kythe, сохраняем
		{"+5", 5},

		// десятичные единицы - степени 1000, регистр не важен
		{"10B", 10},
		{"1kB", 1e3},
		{"1KB", 1e3},
		{"5MB", 5e6},
		{"4GB", 4e9},
		{"100Gb", 100e9}, // "Gb" - это тоже децимальный GB (см. фикстуры тестов)
		{"2TB", 2e12},
		{"1PB", 1e15},

		// двоичные единицы - степени 1024
		{"1KiB", 1 << 10},
		{"1MiB", 1 << 20},
		{"5GiB", 5 << 30},
		{"1TiB", 1 << 40},
		{"1PiB", 1 << 50},

		// дробные с единицей
		{"1.5gb", 15e8},
		{".5kb", 500},
		{"10.mb", 10e6},
	}

	for _, tc := range ok {
		got, err := parseSize(tc.in)
		if err != nil {
			t.Errorf("parseSize(%q): unexpected error: %v", tc.in, err)
			continue
		}

		if got != tc.want {
			t.Errorf("parseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}

	bad := []string{
		"",           // пусто
		"-5",         // отрицательное значение - лимита "минус 5 байт" не бывает
		"-1GB",       // отрицательное с единицей
		"NaN",        // принималось kythe, у нас - ошибка
		"Inf",        // принималось kythe, у нас - ошибка
		"10 MB",      // пробел между числом и единицей
		"xx10mbyy",   // kythe вытаскивал "10mb" из мусора, у нас грамматика якорена
		"10k",        // неизвестная единица
		"5XB",        // неизвестная единица
		"mb",         // единица без числа
		"1e300",      // переполнение uint64
		"99999999pb", // переполнение uint64 через единицу
	}

	for _, in := range bad {
		if got, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) = %d, want error", in, got)
		}
	}
}
