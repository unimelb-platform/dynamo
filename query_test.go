package dynamo

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/guregu/toki"
)

type hit struct {
	User      int `dynamo:"UserID"`
	Date      unixTime
	ContentID string
	Page      int
	SkipThis  string `dynamo:"-"`
	Bonus     *int   `dynamo:",omitempty"`

	TestText  toki.Time
	SkipMePlz time.Time `dynamo:",omitempty"`

	StringSlice []string

	embedMe
	Greeting other

	Features  map[string]bool
	Something interface{}

	Check SuperComplex
}

type embedMe struct {
	Extra bool
}

type other struct {
	Hello string
}

type SuperComplex []struct {
	HelpMe struct {
		FFF []int `dynamo:",set"`
	}
}

func makeSuperComplex() SuperComplex {
	sc := make(SuperComplex, 2)
	sc[0].HelpMe.FFF = []int{1, 2, 3}
	return sc
}

func TestGetCount(t *testing.T) {
	db := testDB()
	hits := db.Table("TestDB")
	q := hits.Get("UserID", 666)
	ct, err := q.Count()
	t.Log("count", ct, err)
	// t.Fail()
}

func TestGetAll(t *testing.T) {
	db := testDB()
	hits := db.Table("TestDB")
	q := hits.Get("UserID", 666)
	// q.Range("Date", Between, 1425279050, 1425279200)
	// q.Range("Date", Equals, 1425279099)
	// q.Consistent(true)
	// q.Project("UserID", "Date", "ContentID", "Page", "Test[1]")
	// q.Order(Descending)

	var records []hit
	err := q.All(&records)

	t.Logf("all %+v %v", records, err)

	for _, r := range records {
		t.Log(r.Date.String())
	}

	// t.Fail()
}

// func TestGetOne(t *testing.T) {
// 	db := testDB()
// 	hits := db.Table("TestDB")

// 	var h hit
// 	err := hits.Get("UserID", 666).Range("Date", Equals, 1447781270).One(&h)
// 	// t.Fatalf("%+v %v", h, err)
// }

type unixTime struct {
	time.Time
}

var _ Unmarshaler = &unixTime{}

func (ut unixTime) MarshalDynamo() (*dynamodb.AttributeValue, error) {
	num := strconv.FormatInt(ut.Unix(), 10)
	av := &dynamodb.AttributeValue{
		N: aws.String(num),
	}
	return av, nil
}

func (ut *unixTime) UnmarshalDynamo(av *dynamodb.AttributeValue) error {
	sec, err := strconv.ParseInt(*av.N, 10, 64)
	if err != nil {
		return err
	}
	*ut = unixTime{time.Unix(sec, 0)}
	return nil
}