package generator

import (
	"testing"

	"github.com/jaswdr/faker"
)

func Test_service_ReplaceStringWithFakerWhenRequested(t *testing.T) {
	type fields struct {
		faker faker.Faker
	}

	type args struct {
		request string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    func(s interface{}) bool
		errMsg  string
		wantErr bool
	}{
		{
			"Get a name string",
			fields{
				faker.New(),
			},
			args{
				"faker.Person().Name()",
			},
			func(s interface{}) bool {
				return len(s.(string)) > 2
			},
			"min len 2",
			false,
		},
		{
			"Get random text with len 10",
			fields{
				faker.New(),
			},
			args{
				"faker.Lorem().Text(100)",
			},
			func(s interface{}) bool {
				return len(s.(string)) > 10
			},
			"exact len 10",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				s := service{
					faker: tt.fields.faker,
				}
				got, err := s.ReplaceStringWithFakerWhenRequested(tt.args.request)
				if (err != nil) != tt.wantErr {
					t.Errorf("ReplaceStringWithFakerWhenRequested() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !tt.want(got) {
					t.Errorf("ReplaceStringWithFakerWhenRequested() got = %v, %s", got, tt.errMsg)
				}
			},
		)
	}
}
