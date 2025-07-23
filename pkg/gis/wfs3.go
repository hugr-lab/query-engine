package gis

import (
	"bytes"
	"encoding/json"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"

	_ "embed"
)

const (
	mimeJSON     = "application/json"
	mimeHTML     = "text/html"
	mimeOpenAPI  = "application/vnd.oai.openapi+json;version=3.0"
	mimeWFS3HTML = "text/html;profile=\"https://www.opengis.org/spec/WFS_FES/3.0/req-classes\""
	mimeWFS3JSON = "application/vnd.oai.openapi+json;profile=\"https://www.opengis.org/spec/WFS_FES/3.0/req-classes\""
)

func (s *Service) wfs3RootHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement WFS3 root handler

	ct := mimeJSON
	accepts := r.Header.Values("Accept")
	if len(accepts) == 1 {
		accepts = strings.Split(accepts[0], ",")
	}
	if slices.Contains(accepts, mimeHTML) {
		ct = mimeHTML
	}
	base := strings.TrimSuffix(baseURL(r), "/wfs") + "/gis/wfs/"
	conURL, err := url.JoinPath(base, "conformance")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	colURL, err := url.JoinPath(base, "collections")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	apiURL, err := url.JoinPath(base, "api")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	content := RootContent{
		Title:       "Hugr WFS",
		Description: "Hugr Web Feature Service",
		Links: []Link{
			{Href: base, Rel: "self", Type: mimeJSON, Title: "WFS Links"},
			{Href: conURL, Rel: "conformance", Type: mimeJSON, Title: "Conformance"},
			{Href: colURL, Rel: "data", Type: mimeJSON, Title: "Collections"},
			{Href: apiURL, Rel: "api", Type: mimeOpenAPI, Title: "API"},
			{Href: apiURL, Rel: "service-desc", Type: mimeJSON, Title: "API"},
			{Href: apiURL, Rel: "service-doc", Type: mimeHTML, Title: "API"},
		},
	}
	w.Header().Set("Content-Type", ct)
	var b []byte
	if ct == mimeHTML {
		b, err = rootWfs3HTML(content)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = w.Write(b)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if ct != mimeJSON {
		http.Error(w, "Unsupported content type: "+ct, http.StatusNotAcceptable)
		return
	}
	err = json.NewEncoder(w).Encode(content)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Service) wfs3ConformanceHandler(w http.ResponseWriter, r *http.Request) {
	ct := mimeJSON
	accepts := r.Header.Values("Accept")
	if len(accepts) == 1 {
		accepts = strings.Split(accepts[0], ",")
	}
	if slices.Contains(accepts, mimeHTML) {
		ct = mimeHTML
	}
	// Content-Type will be set later based on the value of ct
	content := ConformanceClasses{
		ConformsTo: []string{
			"http://www.opengis.net/spec/wfs-1/3.0/req/core",
			"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
			"http://www.opengis.net/spec/wfs-1/3.0/req/geojson",
		},
	}

	w.Header().Set("Content-Type", ct)
	if ct == mimeHTML {
		b, err := wfs3ConformanceHTML(content)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = w.Write(b)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	if ct != mimeJSON {
		http.Error(w, "Unsupported content type: "+ct, http.StatusNotAcceptable)
		return
	}
	err := json.NewEncoder(w).Encode(content)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// --- @See http://raw.githubusercontent.com/opengeospatial/WFS_FES/master/core/openapi/schemas/root.yaml
//
//	for rootContentSchema Definition
//
// What the endpoint at "/" returns
type RootContent struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Links       []Link `json:"links"`
}

// --- @See https://raw.githubusercontent.com/opengeospatial/WFS_FES/master/core/openapi/schemas/bbox.yaml
type Bbox struct {
	Crs  string    `json:"crs"`
	Bbox []float64 `json:"bbox"`
}

// --- @See https://raw.githubusercontent.com/opengeospatial/WFS_FES/master/core/openapi/schemas/link.yaml
//
//	for link schema
type Link struct {
	Href     string `json:"href"`
	Rel      string `json:"rel"`
	Type     string `json:"type"`
	Hreflang string `json:"hreflang"`
	Title    string `json:"title"`
}

// --- @See https://raw.githubusercontent.com/opengeospatial/WFS_FES/master/core/openapi/schemas/collectionInfo.yaml
//
//	for collectionInfo schema
type CollectionInfo struct {
	Name          string   `json:"name"`
	Title         string   `json:"title,omitempty"`
	Description   string   `json:"description,omitempty"`
	Links         []Link   `json:"links"`
	Extent        *Bbox    `json:"extent,omitempty"`
	Crs           []string `json:"crs,omitempty"`
	IsEnabledWFS1 bool     `json:"-"`
}

// --- @See https://raw.githubusercontent.com/opengeospatial/WFS_FES/master/core/openapi/schemas/content.yaml
//
//	for collectionsInfo schema.
type CollectionsInfo struct {
	Links       []Link           `json:"links"`
	Collections []CollectionInfo `json:"collections"`
}

// --- @See https://raw.githubusercontent.com/opengeospatial/WFS_FES/master/core/openapi/schemas/req-classes.yaml
//
//	for ConformanceClasses schema
type ConformanceClasses struct {
	ConformsTo []string `json:"conformsTo"`
}

//go:embed assets/wfs3.html
var rootWfs3Template string

func rootWfs3HTML(content RootContent) ([]byte, error) {
	tmpl, err := template.New("wfs3").Parse(rootWfs3Template)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, content)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//go:embed assets/conformance.html
var conformanceWfs3Template string

func wfs3ConformanceHTML(content ConformanceClasses) ([]byte, error) {
	tmpl, err := template.New("conformance").Parse(conformanceWfs3Template)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, content)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
