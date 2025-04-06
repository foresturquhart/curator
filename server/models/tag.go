package models

import (
	"time"
)

type Tag struct {
	ID          int64     `json:"id"`
	UUID        string    `json:"uuid"`
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	ParentID    *int64    `json:"parent_id,omitempty"`
	Position    int32     `json:"position,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (t *Tag) ToSearchRecord() *TagSearchRecord {
	return &TagSearchRecord{
		ID:          t.ID,
		UUID:        t.UUID,
		Name:        t.Name,
		Description: t.Description,
		ParentID:    t.ParentID,
		CreatedAt:   t.CreatedAt,
		UpdatedAt:   t.UpdatedAt,
	}
}

func (t *Tag) ToCacheFields() map[string]any {
	fields := map[string]any{
		"id":         t.ID,
		"uuid":       t.UUID,
		"name":       t.Name,
		"position":   t.Position,
		"created_at": t.CreatedAt.Format(time.RFC3339),
		"updated_at": t.UpdatedAt.Format(time.RFC3339),
	}

	if t.Description != nil {
		fields["description"] = *t.Description
	} else {
		fields["description"] = ""
	}

	if t.ParentID != nil {
		fields["parent_id"] = *t.ParentID
	} else {
		fields["parent_id"] = ""
	}

	return fields
}

type TagSearchRecord struct {
	ID          int64     `json:"id"`
	UUID        string    `json:"uuid"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
	ParentID    *int64    `json:"parent_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (r *TagSearchRecord) ToModel() *Tag {
	return &Tag{
		ID:          r.ID,
		UUID:        r.UUID,
		Name:        r.Name,
		Description: r.Description,
		ParentID:    r.ParentID,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
	}
}

type TagTreeNode struct {
	Tag      *Tag           `json:"tag"`
	Children []*TagTreeNode `json:"children,omitempty"`
}
