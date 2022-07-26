package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/shoulai/mongodb"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Test struct {
	Id            primitive.ObjectID `bson:"_id"`
	Title         string             `bson:"title"`
	Author        string             `bson:"author"`
	YearPublished int64              `bson:"year_published"`
}

func main() {
	ctx := context.Background()

	//连接到mongodb
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalln(err)
	}

	//设置使用的库和表
	mgdb := mongodb.NewMongodb[Test](client, "test", "favorite_books")

	//插入单条
	insertResult, err := mgdb.InsertOne(ctx, Test{
		Id:            primitive.NewObjectID(),
		Title:         "test",
		Author:        "author test",
		YearPublished: 9999,
	})

	log.Printf("插入单条记录: %v \n", insertResult.InsertedID)

	//插入多条
	var tests []interface{}
	for i := 1; i < 100; i++ {
		tests = append(tests, Test{
			Id:            primitive.NewObjectID(),
			Title:         "test_" + fmt.Sprintf("%d", i),
			Author:        "author test " + fmt.Sprintf("%d", i),
			YearPublished: int64(i),
		})
	}
	insertsResult, err := mgdb.InsertMultiple(ctx, tests)

	log.Printf("插入多条记录: %v \n", insertsResult.InsertedIDs)

	//查询
	filter := mongodb.Newfilter().EQ("title", "test").EQ("author", "author test")
	result, err := mgdb.FindOne(ctx, filter)
	if err != nil {
		log.Fatalf("%s", err)
	}
	buf, err := json.Marshal(result)
	fmt.Printf("查询单条记录: %s\n  ", string(buf))

	//查询
	options := &options.FindOptions{}
	options.SetSkip(2)
	options.SetLimit(6)
	filter = mongodb.Newfilter().GT("year_published", 5).LT("year_published", 10)
	results, err := mgdb.Find(ctx, filter, options)
	if err != nil {
		log.Fatalf("%s", err)
	}
	buf, err = json.Marshal(results)
	fmt.Printf("查询多条记录: %v\n  ", string(buf))

	//单条记录更新
	filter = mongodb.Newfilter().EQ("year_published", 9999)
	updateCount, err := mgdb.UpdateOne(ctx, filter, map[string]interface{}{
		"author": "test 00021",
	})
	if err != nil {
		log.Fatalf("%s", err)

	}
	fmt.Printf("更新数量 : %d\n", updateCount.ModifiedCount)

	//批量更新
	filter = mongodb.Newfilter().IN("year_published", 11, 12, 13)
	updateCount, err = mgdb.UpdateMany(ctx, filter, map[string]interface{}{
		"author": "update author",
	})

	if err != nil {
		log.Fatalf("%s", err)
	}
	fmt.Printf("批量更新数量 : %d\n", updateCount.ModifiedCount)

	//单机不支持事务
	// err = mgdb.UseSession(context.TODO(), func(sc mongo.SessionContext) error {
	// 	sc.StartTransaction()
	// 	//单条数据删除
	// 	filter = mongodb.Newfilter().EQ("year_published", 9999)
	// 	_, err := mgdb.DeleteOne(sc, filter)
	// 	// if err == nil {
	// 	// 	fmt.Printf("-----单条数据删除数量(设置一个错误)---- : %d\n", deleteCount)
	// 	// 	err = errors.New("test err")
	// 	// }

	// 	if err != nil {
	// 		log.Printf("删除异常: %v", err)
	// 		if err = sc.AbortTransaction(sc); err != nil {
	// 			log.Println("删除，事务回滚失败")
	// 			return err
	// 		}
	// 		return err
	// 	}
	// 	return sc.CommitTransaction(sc)
	// })
	// if err != nil {
	// 	log.Printf("事务处理结果失败原因:%s", err)
	// }
	//单条数据删除
	filter = mongodb.Newfilter().EQ("year_published", 15)
	deleteCount, err := mgdb.DeleteOne(ctx, filter)
	if err != nil {
		log.Fatalf("%s", err)
	}
	fmt.Printf("单条数据删除数量 : %d\n", deleteCount.DeletedCount)

	//多条数据删除
	filter = mongodb.Newfilter().IN("year_published", 16, 17, 18)
	deleteCount, err = mgdb.DeleteMany(ctx, filter)
	if err != nil {
		log.Fatalf("%s", err)
	}
	fmt.Printf("多条数据删除数量 : %d\n", deleteCount.DeletedCount)

}
