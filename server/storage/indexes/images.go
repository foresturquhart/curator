package indexes

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/foresturquhart/curator/server/utils"
)

func init() {
	Indexes["images"] = &types.TypeMapping{
		Properties: map[string]types.Property{
			"id":   types.LongNumberProperty{},
			"uuid": types.KeywordProperty{},
			"filename": types.TextProperty{
				Fields: map[string]types.Property{
					"keyword": types.KeywordProperty{
						IgnoreAbove: utils.NewPointer(256),
					},
				},
			},
			"md5":    types.KeywordProperty{},
			"sha1":   types.KeywordProperty{},
			"width":  types.IntegerNumberProperty{},
			"height": types.IntegerNumberProperty{},
			"format": types.KeywordProperty{},
			"size":   types.LongNumberProperty{},
			"title": types.TextProperty{
				Analyzer: utils.NewPointer("english"),
				Fields: map[string]types.Property{
					"keyword": types.KeywordProperty{
						IgnoreAbove: utils.NewPointer(256),
					},
				},
			},
			"description": types.TextProperty{
				Analyzer: utils.NewPointer("english"),
				Fields: map[string]types.Property{
					"keyword": types.KeywordProperty{
						IgnoreAbove: utils.NewPointer(256),
					},
				},
			},
			"created_at": types.DateProperty{},
			"updated_at": types.DateProperty{},

			// Nested properties
			"tags": types.NestedProperty{
				Properties: map[string]types.Property{
					"id":   types.LongNumberProperty{},
					"uuid": types.KeywordProperty{},
					"name": types.KeywordProperty{},
					"description": types.TextProperty{
						Analyzer: utils.NewPointer("english"),
						Fields: map[string]types.Property{
							"keyword": types.KeywordProperty{
								IgnoreAbove: utils.NewPointer(256),
							},
						},
					},
					"added_at": types.DateProperty{},
				},
			},
			"people": types.NestedProperty{
				Properties: map[string]types.Property{
					"id":   types.LongNumberProperty{},
					"uuid": types.KeywordProperty{},
					"name": types.TextProperty{
						Analyzer: utils.NewPointer("english"),
						Fields: map[string]types.Property{
							"keyword": types.KeywordProperty{
								IgnoreAbove: utils.NewPointer(256),
							},
						},
					},
					"description": types.TextProperty{
						Analyzer: utils.NewPointer("english"),
						Fields: map[string]types.Property{
							"keyword": types.KeywordProperty{
								IgnoreAbove: utils.NewPointer(256),
							},
						},
					},
					"role":     types.KeywordProperty{},
					"added_at": types.DateProperty{},
				},
			},
			"sources": types.NestedProperty{
				Properties: map[string]types.Property{
					"url": types.TextProperty{
						Analyzer: utils.NewPointer("english"),
						Fields: map[string]types.Property{
							"keyword": types.KeywordProperty{
								IgnoreAbove: utils.NewPointer(256),
							},
						},
					},
					"name": types.TextProperty{
						Analyzer: utils.NewPointer("english"),
						Fields: map[string]types.Property{
							"keyword": types.KeywordProperty{
								IgnoreAbove: utils.NewPointer(256),
							},
						},
					},
					"description": types.TextProperty{
						Analyzer: utils.NewPointer("english"),
						Fields: map[string]types.Property{
							"keyword": types.KeywordProperty{
								IgnoreAbove: utils.NewPointer(256),
							},
						},
					},
				},
			},

			// Computed properties
			"pixel_count": types.LongNumberProperty{},
			"tags_count":  types.IntegerNumberProperty{},
		},
	}
}
