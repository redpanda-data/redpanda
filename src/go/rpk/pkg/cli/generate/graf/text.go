// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

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
