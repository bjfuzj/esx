package esx

import "bytes"

func nextStd(s string) (string, string) {
	sLen := len(s)
	ss := string(s[0])
	switch ss {
	case "A":
		if sLen >= 2 && s[:2] == "AM" {
			return "PM", s[2:]
		}
		return "A", s[1:]

	case "a":
		if sLen >= 2 && s[:2] == "am" {
			return "pm", s[2:]
		}
		return "a", s[1:]

	case "y":
		if sLen >= 4 && s[:4] == "yyyy" {
			return "2006", s[4:]
		}
		if sLen >= 2 && s[:2] == "yy" {
			return "06", s[2:]
		}

		return "y", s[1:]

	case "M":
		if sLen >= 4 && s[:4] == "MMMM" {
			return "January", s[4:]
		}
		if sLen >= 3 && s[:3] == "MMM" {
			return "Jan", s[3:]
		}
		if sLen >= 2 && s[:2] == "MM" {
			return "01", s[2:]
		}
		return "1", s[1:]

	case "d":
		if sLen >= 2 && s[:2] == "dd" {
			return "02", s[2:]
		}
		return "2", s[1:]

	case "H":
		if sLen >= 2 && s[:2] == "HH" {
			return "15", s[2:]
		}
		return "3", s[1:]

	case "m":
		if sLen >= 2 && s[:2] == "mm" {
			return "04", s[2:]
		}
		return "4", s[1:]

	case "s":
		if sLen >= 2 && s[:2] == "ss" {
			return "05", s[2:]
		}
		return "5", s[1:]

	case "S":
		idx := 0
		layout := bytes.NewBufferString("")
		for _, x := range s {
			if string(x) != "S" {
				break
			}
			idx++
			layout.WriteString("0")
		}

		return layout.String(), s[idx:]

	case "E":
		if sLen >= 4 && s[:4] == "EEEE" {
			return "Monday", s[4:]
		}

		if sLen >= 2 && s[:2] == "EE" {
			return "Mon", s[2:]
		}

		return "E", s[1:]

	default:
		return ss, s[1:]
	}
}

// GetTimeTemps 将Java的时间格式符转变成Go的
func GetTimeTemps(s string) string {
	if s == "UNIX" {
		return "UNIX"
	}

	if len(s) == 0 {
		return ""
	}

	ret := bytes.NewBufferString("")
	var currStd string
	for {
		currStd, s = nextStd(s)
		ret.WriteString(currStd)
		if len(s) == 0 {
			break
		}
	}

	return ret.String()
}
