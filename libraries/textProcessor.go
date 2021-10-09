package libraries

import (
	"fmt"
	"strings"
)

type Text struct {
	Body                string
	Lenght              int
	WordCount           int
	Plurals             int
	Singulars           int
	Words               []string
	Letters             []string
	ContainsPunctations bool
	CapitalizedBody     string
}

func (text *Text) Process() {
	words := strings.Fields(text.Body)

	text.Lenght = len(text.Body)
	text.WordCount = len(words)
	text.Plurals = processPlurals(words)
	text.Singulars = len(words) - processPlurals(words)
	text.Words = words
	text.Letters = strings.Split(text.Body, "")
	text.ContainsPunctations = checkForPunctuation(text.Body)
	text.CapitalizedBody = strings.Title(text.Body)
}

func processPlurals(words []string) int {
	amount := 0
	for _, v := range words {
		if v[len(v)-1:] == "s" {
			amount += 1
		}
	}
	return amount
}

func checkForPunctuation(textBody string) bool {
	punctuationSigns := ".,/;':\"?><!@#$%^&*()_-+=~`"
	for _, v := range strings.Split(punctuationSigns, "") {
		if strings.Contains(textBody, v) {
			return true
		}
	}
	return false
}

func (text Text) String() string {
	return fmt.Sprintf("Text information:\n\tBody Extract: %s\n\tNumber of letters: %d\n\tNumber of words: %d\n\tNumber of plural words: %d\n\tNumber of singular words: %d\n\tFirst 5 words: %s\n\tFirst 5 letters: %s\n\tHas punctuations: %t\n\tCapitalized body: %s\n\t",
		text.Body[0:30],
		text.Lenght,
		text.WordCount,
		text.Plurals,
		text.Singulars,
		text.Words[0:5],
		text.Letters[0:5],
		text.ContainsPunctations,
		text.CapitalizedBody[0:30],
	)
}
