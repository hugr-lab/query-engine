package adminui

import (
	"bytes"
	"html/template"
	"net/http"

	_ "embed"
)

//go:embed assets/graphiql.html
var graphiqlHTML string

type Config struct {
	Path string
}

func AdminUIHandler(config Config) (func(w http.ResponseWriter, r *http.Request), error) {
	page, err := fromTemplate(map[string]any{
		"graphQLPath": config.Path,
	}, graphiqlHTML)
	if err != nil {
		return nil, err
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, err = w.Write(page)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}, nil
}

func fromTemplate(config map[string]any, content string) ([]byte, error) {
	var b bytes.Buffer
	t := template.New("template")
	t, err := t.Parse(content)
	if err != nil {
		return nil, err
	}
	err = t.Execute(&b, config)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}
