package indexes

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/foresturquhart/curator/server/utils"
)

func init() {
	Indexes["people"] = &types.TypeMapping{
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
			"created_at": types.DateProperty{},
			"updated_at": types.DateProperty{},
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
		},
	}
}
