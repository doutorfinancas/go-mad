package generator

import (
	"errors"
	"fmt"
	"github.com/jaswdr/faker"
	"reflect"
	"strconv"
	"strings"
)

type Service interface {
	ReplaceStringWithFakerWhenRequested(request string) (string, error)
}

type service struct {
	faker faker.Faker
}

func NewService() Service {
	return &service{
		faker: faker.New(),
	}
}

func (s service) ReplaceStringWithFakerWhenRequested(request string) (string, error) {
	if len(request) < 5 || request[0:5] != "faker" {
		return request, nil
	}

	a := strings.Split(request, ".")
	if len(a) == 1 {
		return request,
			errors.New("requires more arguments to get a faker func")
	}

	var res []reflect.Value
	var err error

	if res, err = callMethod(&s.faker, strings.Split(a[1], "(")[0], nil); err != nil {
		return "", err
	}

	if len(a) == 2 {
		return stringify(res[0]), nil
	}

	argsString := strings.Split(a[2], "(")

	obj := reflect.Indirect(res[0]).Interface()

	args, err := getMethodArguments(
		obj,
		argsString[0],
		strings.Replace(argsString[1], ")", "", -1))
	// faker as a max depth of 2, here should be last
	// @todo this analysis should be dynamic
	if res, err = callMethod(obj, argsString[0], args); err != nil {
		return "", err
	}

	return stringify(res[0]), nil
}

func getMethodArguments(
	entry interface{},
	name string,
	argsString string,
) ([]reflect.Value, error) {
	_, exists := reflect.TypeOf(entry).MethodByName(name)

	if !exists {
		return nil,
			errors.New("could not find faker func")
	}

	method := reflect.ValueOf(entry).
		MethodByName(name)

	args := strings.Split(argsString, ",")

	if len(args) != method.Type().NumIn() {
		return nil, errors.New(
			fmt.Sprintf(
				"number of arguments to call %s is %d and we got %d",
				name,
				method.Type().NumIn(),
				len(args),
			))
	}

	in := make([]reflect.Value, method.Type().NumIn())

	for i := 0; i < method.Type().NumIn(); i++ {
		t := method.Type().In(i)
		in[i] = reflect.ValueOf(convert(t, clearString(args[i]))).Convert(t)
	}

	return in, nil
}

func callMethod(
	entry interface{},
	name string,
	objects []reflect.Value,
) ([]reflect.Value, error) {
	_, exists := reflect.TypeOf(entry).MethodByName(name)

	if !exists {
		return nil,
			errors.New("could not find faker func")
	}

	return reflect.ValueOf(entry).
		MethodByName(name).
		Call(objects), nil
}

func clearString(str string) string {
	s := strings.Replace(str, "\"", "", -1)
	return strings.Replace(s, " ", "", -1)
}

func stringify(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func convert(t reflect.Type, s string) interface{} {
	switch t.Kind() {
	case reflect.Int, reflect.Int64:
		v, _ := strconv.Atoi(s)
		return v
	case reflect.Float64, reflect.Float32:
		v, _ := strconv.ParseFloat(s, 64)
		return v
	default:
		return s
	}
}
