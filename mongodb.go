package mongodb

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mongodb[T any] struct {
	client     *mongo.Client
	database   string
	collection string
}

func NewMongodb[T any](client *mongo.Client, database string, collection string) *Mongodb[T] {
	return &Mongodb[T]{
		client,
		database,
		collection,
	}
}

//新增一条记录
func (mg *Mongodb[T]) InsertOne(ctx context.Context, insert T, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	return mg.getCollection().InsertOne(ctx, insert, opts...)
}

//新增多条记录
func (mg *Mongodb[T]) InsertMultiple(ctx context.Context, insert []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	return mg.getCollection().InsertMany(ctx, insert, opts...)
}

//根据字段名和值查询一条记录
func (mg *Mongodb[T]) FindOne(ctx context.Context, filter filter, opts ...*options.FindOneOptions) (T, error) {
	var t T
	err := mg.getCollection().FindOne(ctx, filter, opts...).Decode(&t)
	if err != nil {
		return t, err
	}
	return t, nil
}

//根据条件查询多条记录
func (mg *Mongodb[T]) Find(ctx context.Context, filter filter, opts ...*options.FindOptions) ([]T, error) {
	cursor, err := mg.getCollection().Find(ctx, filter, opts...)
	var ts []T
	if err != nil {
		return ts, err
	}
	for cursor.Next(ctx) {
		var t T
		err := cursor.Decode(&t)
		if err != nil {
			return ts, err
		}
		ts = append(ts, t)
	}
	return ts, nil
}

//根据条件更新
func (mg *Mongodb[T]) UpdateOne(ctx context.Context, filter filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return mg.getCollection().UpdateOne(ctx, filter, bson.M{"$set": update}, opts...)
}

//更新多个
func (mg *Mongodb[T]) UpdateMany(ctx context.Context, filter filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return mg.getCollection().UpdateMany(ctx, filter, bson.D{{Key: "$set", Value: update}}, opts...)
}

//删除一条记录
func (mg *Mongodb[T]) DeleteOne(ctx context.Context, filter filter, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	result, err := mg.getCollection().DeleteOne(ctx, filter, opts...)
	return result, err
}

//删除多条记录
func (mg *Mongodb[T]) DeleteMany(ctx context.Context, filter filter, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return mg.getCollection().DeleteMany(ctx, filter, opts...)
}

//获取表
func (mg *Mongodb[T]) getCollection() *mongo.Collection {
	return mg.client.Database(mg.database).Collection(mg.collection)
}

//会话
func (mg *Mongodb[T]) UseSession(ctx context.Context, fn func(mongo.SessionContext) error) error {
	return mg.client.UseSessionWithOptions(ctx, options.Session(), fn)
}

//objcetid
func (mg *Mongodb[T]) ObjectID(id string) primitive.ObjectID {
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		log.Fatal(err)
	}
	return objectId
}

//定义过滤器
type filter bson.D

//匹配字段值大于指定值的文档
func (f filter) GT(key string, value interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$gt", Value: value}}})
	return f
}

//匹配字段值大于等于指定值的文档
func (f filter) GTE(key string, value interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$gte", Value: value}}})
	return f
}

//匹配字段值等于指定值的文档
func (f filter) EQ(key string, value interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$eq", Value: value}}})
	return f
}

//匹配字段值小于指定值的文档
func (f filter) LT(key string, value interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$lt", Value: value}}})
	return f
}

//匹配字段值小于等于指定值的文档
func (f filter) LET(key string, value interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$let", Value: value}}})
	return f
}

//匹配字段值不等于指定值的文档，包括没有这个字段的文档
func (f filter) NE(key string, value interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$ne", Value: value}}})
	return f
}

//匹配字段值等于指定数组中的任何值
func (f filter) IN(key string, value ...interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$in", Value: value}}})
	return f
}

//字段值不在指定数组或者不存在
func (f filter) NIN(key string, value ...interface{}) filter {
	f = append(f, bson.E{Key: key, Value: bson.D{{Key: "$nin", Value: value}}})
	return f
}

//自定义匹配条件
func (f filter) And(key string, op string, value interface{}) filter {
	return append(f, bson.E{Key: key, Value: bson.D{{Key: op, Value: value}}})
}

//创建一个条件查询对象
func Newfilter() filter {
	return filter{}
}
