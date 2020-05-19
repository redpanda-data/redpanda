package graf

import "encoding/json"

type TextPanel struct {
	BasePanel
	Content string `json:"content"`
	Mode    string `json:"mode"`
}

func (*TextPanel) Type() string {
	return "text"
}

func (p *TextPanel) GetGridPos() *GridPos {
	return &p.GridPos
}

func (p *TextPanel) MarshalJSON() ([]byte, error) {
	type PanelAlias TextPanel
	typedPanel := struct {
		Type string `json:"type"`
		PanelAlias
	}{
		p.Type(),
		(PanelAlias)(*p),
	}
	return json.Marshal(typedPanel)
}

func NewTextPanel(content, mode string) *TextPanel {
	return &TextPanel{
		BasePanel: BasePanel{
			ID:       nextID(),
			Editable: true,
			GridPos:  GridPos{W: 8, H: 8},
			Span:     1,
		},
		Content: content,
		Mode:    mode,
	}
}
