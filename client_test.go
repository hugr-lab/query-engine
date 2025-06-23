package hugr

import "testing"

func TestClientQuery(t *testing.T) {
	// Create a new client
	client := NewClient("http://localhost:15003/ipc")

	res, err := client.Query(t.Context(), `
	{
		utils {
			path_view(
			args: {path: "/Users/vgribanov/projects/hugr-lab/schemes/test-source/*.graphql11"}
			) {
			name
			content
			}
		}
	}`, nil)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer res.Close()
	var data []struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}
	err = res.ScanData("utils.path_view", &data)
	if err != nil {
		t.Fatalf("Failed to scan data: %v", err)
	}
	for _, d := range data {
		t.Logf("Name: %s, Content: %s", d.Name, d.Content)
	}
}
