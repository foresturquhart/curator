package indexes

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/foresturquhart/curator/server/utils"
)

func init() {
	Indexes["tags"] = &types.TypeMapping{
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
		},
	}
}
