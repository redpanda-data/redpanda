package graf

import "encoding/json"

type RowPanel struct {
	BasePanel
	Panels []Panel `json:"panels"`
}

func (RowPanel) Type() string {
	return "row"
}

func (p RowPanel) MarshalJSON() ([]byte, error) {
	type PanelAlias RowPanel
	typedPanel := struct {
		Type string `json:"type"`
		PanelAlias
	}{
		p.Type(),
		(PanelAlias)(p),
	}
	return json.Marshal(typedPanel)
}

func NewRowPanel(title string, panels ...Panel) RowPanel {
	return RowPanel{
		BasePanel: BasePanel{
			ID:       nextID(),
			Title:    title,
			Editable: true,
			GridPos:  GridPos{W: 24, H: 1},
		},
		Panels: panels,
	}
}
