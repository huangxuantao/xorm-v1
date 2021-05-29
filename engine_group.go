// Copyright 2017 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"context"
	"database/sql"
	"io"
	"reflect"
	"time"

	"xorm.io/xorm/caches"
	"xorm.io/xorm/contexts"
	"xorm.io/xorm/dialects"
	"xorm.io/xorm/log"
	"xorm.io/xorm/names"
)

// EngineGroup defines an engine group
type EngineGroup struct {
	*Engine
	slaves []*Engine
	policy GroupPolicy
}

// NewEngineGroup creates a new engine group
func NewEngineGroup(args1 interface{}, args2 interface{}, policies ...GroupPolicy) (*EngineGroup, error) {
	var eg EngineGroup
	if len(policies) > 0 {
		eg.policy = policies[0]
	} else {
		eg.policy = RoundRobinPolicy()
	}

	driverName, ok1 := args1.(string)
	conns, ok2 := args2.([]string)
	if ok1 && ok2 {
		engines := make([]*Engine, len(conns))
		for i, conn := range conns {
			engine, err := NewEngine(driverName, conn)
			if err != nil {
				return nil, err
			}
			engine.engineGroup = &eg
			engines[i] = engine
		}

		eg.Engine = engines[0]
		eg.slaves = engines[1:]

		eg.Check()
		return &eg, nil
	}

	master, ok3 := args1.(*Engine)
	slaves, ok4 := args2.([]*Engine)
	if ok3 && ok4 {
		master.engineGroup = &eg
		for i := 0; i < len(slaves); i++ {
			slaves[i].engineGroup = &eg
		}
		eg.Engine = master
		eg.slaves = slaves

		eg.Check()
		return &eg, nil
	}
	return nil, ErrParamsType
}

// Close the engine
func (eg *EngineGroup) Close() error {
	err := eg.Engine.Close()
	if err != nil {
		return err
	}

	for i := 0; i < len(eg.slaves); i++ {
		err := eg.slaves[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// ContextHook returned a group session
func (eg *EngineGroup) Context(ctx context.Context) *Session {
	sess := eg.NewSession()
	sess.isAutoClose = true
	return sess.Context(ctx)
}

// NewSession returned a group session
func (eg *EngineGroup) NewSession() *Session {
	sess := eg.Engine.NewSession()
	sess.sessionType = groupSession
	return sess
}

// Master returns the master engine
func (eg *EngineGroup) Master() *Engine {
	return eg.Engine
}

// Ping tests if database is alive
func (eg *EngineGroup) Ping() error {
	if err := eg.Engine.Ping(); err != nil {
		return err
	}

	for _, slave := range eg.slaves {
		if err := slave.Ping(); err != nil {
			return err
		}
	}
	return nil
}

func (eg *EngineGroup) Check() {
	if err := eg.Engine.Ping(); err != nil {
		eg.Engine.live = false
	} else {
		eg.Engine.live = true
	}

	for _, slave := range eg.slaves {
		if err := slave.Ping(); err != nil {
			slave.live = false
		} else {
			slave.live = true
		}
	}
}

// SetColumnMapper set the column name mapping rule
func (eg *EngineGroup) SetColumnMapper(mapper names.Mapper) {
	eg.Engine.SetColumnMapper(mapper)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetColumnMapper(mapper)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
func (eg *EngineGroup) SetConnMaxLifetime(d time.Duration) {
	eg.Engine.SetConnMaxLifetime(d)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetConnMaxLifetime(d)
	}
}

// SetDefaultCacher set the default cacher
func (eg *EngineGroup) SetDefaultCacher(cacher caches.Cacher) {
	eg.Engine.SetDefaultCacher(cacher)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetDefaultCacher(cacher)
	}
}

// SetLogger set the new logger
func (eg *EngineGroup) SetLogger(logger interface{}) {
	eg.Engine.SetLogger(logger)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetLogger(logger)
	}
}

func (eg *EngineGroup) AddHook(hook contexts.Hook) {
	eg.Engine.AddHook(hook)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].AddHook(hook)
	}
}

// SetLogLevel sets the logger level
func (eg *EngineGroup) SetLogLevel(level log.LogLevel) {
	eg.Engine.SetLogLevel(level)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetLogLevel(level)
	}
}

// SetMapper set the name mapping rules
func (eg *EngineGroup) SetMapper(mapper names.Mapper) {
	eg.Engine.SetMapper(mapper)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetMapper(mapper)
	}
}

// SetMaxIdleConns set the max idle connections on pool, default is 2
func (eg *EngineGroup) SetMaxIdleConns(conns int) {
	eg.Engine.DB().SetMaxIdleConns(conns)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].DB().SetMaxIdleConns(conns)
	}
}

// SetMaxOpenConns is only available for go 1.2+
func (eg *EngineGroup) SetMaxOpenConns(conns int) {
	eg.Engine.DB().SetMaxOpenConns(conns)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].DB().SetMaxOpenConns(conns)
	}
}

// SetPolicy set the group policy
func (eg *EngineGroup) SetPolicy(policy GroupPolicy) *EngineGroup {
	eg.policy = policy
	return eg
}

// SetQuotePolicy sets the special quote policy
func (eg *EngineGroup) SetQuotePolicy(quotePolicy dialects.QuotePolicy) {
	eg.Engine.SetQuotePolicy(quotePolicy)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetQuotePolicy(quotePolicy)
	}
}

// SetTableMapper set the table name mapping rule
func (eg *EngineGroup) SetTableMapper(mapper names.Mapper) {
	eg.Engine.SetTableMapper(mapper)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].SetTableMapper(mapper)
	}
}

// ShowSQL show SQL statement or not on logger if log level is great than INFO
func (eg *EngineGroup) ShowSQL(show ...bool) {
	eg.Engine.ShowSQL(show...)
	for i := 0; i < len(eg.slaves); i++ {
		eg.slaves[i].ShowSQL(show...)
	}
}

// Slave returns one of the physical databases which is a slave according the policy
func (eg *EngineGroup) Slave() *Engine {
	switch len(eg.slaves) {
	case 0:
		return eg.Engine
	//case 1: // 屏蔽这里是考虑使用自定义策略MasterFirst时，保证在Master正常时能走Master，不会因为Slave个数为1，而直接从Slave读取
	//	return eg.slaves[0]
	}
	return eg.policy.Slave(eg)
}

// Slaves returns all the slaves
func (eg *EngineGroup) Slaves() []*Engine {
	return eg.slaves
}

// Cascade use cascade or not
func (eg *EngineGroup) Cascade(trueOrFalse ...bool) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Cascade(trueOrFalse...)
}

// Where method provide a condition query
func (eg *EngineGroup) Where(query interface{}, args ...interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Where(query, args...)
}

// ID method provoide a condition as (id) = ?
func (eg *EngineGroup) ID(id interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.ID(id)
}

// Before apply before Processor, affected bean is passed to closure arg
func (eg *EngineGroup) Before(closures func(interface{})) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Before(closures)
}

// After apply after insert Processor, affected bean is passed to closure arg
func (eg *EngineGroup) After(closures func(interface{})) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.After(closures)
}

// Charset set charset when create table, only support mysql now
func (eg *EngineGroup) Charset(charset string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Charset(charset)
}

// StoreEngine set store engine when create table, only support mysql now
func (eg *EngineGroup) StoreEngine(storeEngine string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.StoreEngine(storeEngine)
}

// Distinct use for distinct columns. Caution: when you are using cache,
// distinct will not be cached because cache system need id,
// but distinct will not provide id
func (eg *EngineGroup) Distinct(columns ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Distinct(columns...)
}

// Select customerize your select columns or contents
func (eg *EngineGroup) Select(str string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Select(str)
}

// Cols only use the parameters as select or update columns
func (eg *EngineGroup) Cols(columns ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Cols(columns...)
}

// AllCols indicates that all columns should be use
func (eg *EngineGroup) AllCols() *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.AllCols()
}

// MustCols specify some columns must use even if they are empty
func (eg *EngineGroup) MustCols(columns ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.MustCols(columns...)
}

// UseBool xorm automatically retrieve condition according struct, but
// if struct has bool field, it will ignore them. So use UseBool
// to tell system to do not ignore them.
// If no parameters, it will use all the bool field of struct, or
// it will use parameters's columns
func (eg *EngineGroup) UseBool(columns ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.UseBool(columns...)
}

// Omit only not use the parameters as select or update columns
func (eg *EngineGroup) Omit(columns ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Omit(columns...)
}

// Nullable set null when column is zero-value and nullable for update
func (eg *EngineGroup) Nullable(columns ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Nullable(columns...)
}

// In will generate "column IN (?, ?)"
func (eg *EngineGroup) In(column string, args ...interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.In(column, args...)
}

// NotIn will generate "column NOT IN (?, ?)"
func (eg *EngineGroup) NotIn(column string, args ...interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.NotIn(column, args...)
}

// Incr provides a update string like "column = column + ?"
func (eg *EngineGroup) Incr(column string, arg ...interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Incr(column, arg...)
}

// Decr provides a update string like "column = column - ?"
func (eg *EngineGroup) Decr(column string, arg ...interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Decr(column, arg...)
}

// SetExpr provides a update string like "column = {expression}"
func (eg *EngineGroup) SetExpr(column string, expression interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.SetExpr(column, expression)
}

// Table temporarily change the Get, Find, Update's table
func (eg *EngineGroup) Table(tableNameOrBean interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Table(tableNameOrBean)
}

// Alias set the table alias
func (eg *EngineGroup) Alias(alias string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Alias(alias)
}

// Limit will generate "LIMIT start, limit"
func (eg *EngineGroup) Limit(limit int, start ...int) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Limit(limit, start...)
}

// Desc will generate "ORDER BY column1 DESC, column2 DESC"
func (eg *EngineGroup) Desc(colNames ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Desc(colNames...)
}

// Asc will generate "ORDER BY column1,column2 Asc"
// This method can chainable use.
//
//        engine.Desc("name").Asc("age").Find(&users)
//        // SELECT * FROM user ORDER BY name DESC, age ASC
//
func (eg *EngineGroup) Asc(colNames ...string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Asc(colNames...)
}

// OrderBy will generate "ORDER BY order"
func (eg *EngineGroup) OrderBy(order string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.OrderBy(order)
}

// Prepare enables prepare statement
func (eg *EngineGroup) Prepare() *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Prepare()
}

// Join the join_operator should be one of INNER, LEFT OUTER, CROSS etc - this will be prepended to JOIN
func (eg *EngineGroup) Join(joinOperator string, tablename interface{}, condition string, args ...interface{}) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Join(joinOperator, tablename, condition, args...)
}

// GroupBy generate group by statement
func (eg *EngineGroup) GroupBy(keys string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.GroupBy(keys)
}

// Having generate having statement
func (eg *EngineGroup) Having(conditions string) *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Having(conditions)
}

// IsTableEmpty if a table has any reocrd
func (eg *EngineGroup) IsTableEmpty(bean interface{}) (bool, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.IsTableEmpty(bean)
}

// IsTableExist if a table is exist
func (eg *EngineGroup) IsTableExist(beanOrTableName interface{}) (bool, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.IsTableExist(beanOrTableName)
}

// TableName returns table name with schema prefix if has
func (eg *EngineGroup) TableName(bean interface{}, includeSchema ...bool) string {
	return dialects.FullTableName(eg.dialect, eg.GetTableMapper(), bean, includeSchema...)
}

// CreateIndexes create indexes
func (eg *EngineGroup) CreateIndexes(bean interface{}) error {
	session := eg.NewSession()
	defer session.Close()
	return session.CreateIndexes(bean)
}

// CreateUniques create uniques
func (eg *EngineGroup) CreateUniques(bean interface{}) error {
	session := eg.NewSession()
	defer session.Close()
	return session.CreateUniques(bean)
}

// ClearCacheBean if enabled cache, clear the cache bean
func (eg *EngineGroup) ClearCacheBean(bean interface{}, id string) error {
	tableName := dialects.FullTableName(eg.dialect, eg.GetTableMapper(), bean)
	cacher := eg.GetCacher(tableName)
	if cacher != nil {
		cacher.ClearIds(tableName)
		cacher.DelBean(tableName, id)
	}
	return nil
}

// ClearCache if enabled cache, clear some tables' cache
func (eg *EngineGroup) ClearCache(beans ...interface{}) error {
	for _, bean := range beans {
		tableName := dialects.FullTableName(eg.dialect, eg.GetTableMapper(), bean)
		cacher := eg.GetCacher(tableName)
		if cacher != nil {
			cacher.ClearIds(tableName)
			cacher.ClearBeans(tableName)
		}
	}
	return nil
}

// UnMapType remove table from tables cache
func (eg *EngineGroup) UnMapType(t reflect.Type) {
	eg.tagParser.ClearCacheTable(t)
}

// Sync2 synchronize structs to database tables
func (eg *EngineGroup) Sync2(beans ...interface{}) error {
	s := eg.NewSession()
	defer s.Close()
	return s.Sync2(beans...)
}

// CreateTables create tabls according bean
func (eg *EngineGroup) CreateTables(beans ...interface{}) error {
	session := eg.NewSession()
	defer session.Close()

	err := session.Begin()
	if err != nil {
		return err
	}

	for _, bean := range beans {
		err = session.createTable(bean)
		if err != nil {
			session.Rollback()
			return err
		}
	}
	return session.Commit()
}

// DropTables drop specify tables
func (eg *EngineGroup) DropTables(beans ...interface{}) error {
	session := eg.NewSession()
	defer session.Close()

	err := session.Begin()
	if err != nil {
		return err
	}

	for _, bean := range beans {
		err = session.dropTable(bean)
		if err != nil {
			session.Rollback()
			return err
		}
	}
	return session.Commit()
}

// DropIndexes drop indexes of a table
func (eg *EngineGroup) DropIndexes(bean interface{}) error {
	session := eg.NewSession()
	defer session.Close()
	return session.DropIndexes(bean)
}

// Exec raw sql
func (eg *EngineGroup) Exec(sqlOrArgs ...interface{}) (sql.Result, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Exec(sqlOrArgs...)
}

// Query a raw sql and return records as []map[string][]byte
func (eg *EngineGroup) Query(sqlOrArgs ...interface{}) (resultsSlice []map[string][]byte, err error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Query(sqlOrArgs...)
}

// QueryString runs a raw sql and return records as []map[string]string
func (eg *EngineGroup) QueryString(sqlOrArgs ...interface{}) ([]map[string]string, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.QueryString(sqlOrArgs...)
}

// QueryInterface runs a raw sql and return records as []map[string]interface{}
func (eg *EngineGroup) QueryInterface(sqlOrArgs ...interface{}) ([]map[string]interface{}, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.QueryInterface(sqlOrArgs...)
}

// Insert one or more records
func (eg *EngineGroup) Insert(beans ...interface{}) (int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Insert(beans...)
}

// InsertOne insert only one record
func (eg *EngineGroup) InsertOne(bean interface{}) (int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.InsertOne(bean)
}

// Update records, bean's non-empty fields are updated contents,
// condiBean' non-empty filds are conditions
// CAUTION:
//        1.bool will defaultly be updated content nor conditions
//         You should call UseBool if you have bool to use.
//        2.float32 & float64 may be not inexact as conditions
func (eg *EngineGroup) Update(bean interface{}, condiBeans ...interface{}) (int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Update(bean, condiBeans...)
}

// Delete records, bean's non-empty fields are conditions
func (eg *EngineGroup) Delete(bean interface{}) (int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Delete(bean)
}

// Get retrieve one record from table, bean's non-empty fields
// are conditions
func (eg *EngineGroup) Get(bean interface{}) (bool, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Get(bean)
}

// Exist returns true if the record exist otherwise return false
func (eg *EngineGroup) Exist(bean ...interface{}) (bool, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Exist(bean...)
}

// Find retrieve records from table, condiBeans's non-empty fields
// are conditions. beans could be []Struct, []*Struct, map[int64]Struct
// map[int64]*Struct
func (eg *EngineGroup) Find(beans interface{}, condiBeans ...interface{}) error {
	session := eg.NewSession()
	defer session.Close()
	return session.Find(beans, condiBeans...)
}

// FindAndCount find the results and also return the counts
func (eg *EngineGroup) FindAndCount(rowsSlicePtr interface{}, condiBean ...interface{}) (int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.FindAndCount(rowsSlicePtr, condiBean...)
}

// Iterate record by record handle records from table, bean's non-empty fields
// are conditions.
func (eg *EngineGroup) Iterate(bean interface{}, fun IterFunc) error {
	session := eg.NewSession()
	defer session.Close()
	return session.Iterate(bean, fun)
}

// Rows return sql.Rows compatible Rows obj, as a forward Iterator object for iterating record by record, bean's non-empty fields
// are conditions.
func (eg *EngineGroup) Rows(bean interface{}) (*Rows, error) {
	session := eg.NewSession()
	return session.Rows(bean)
}

// Count counts the records. bean's non-empty fields are conditions.
func (eg *EngineGroup) Count(bean ...interface{}) (int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Count(bean...)
}

// Sum sum the records by some column. bean's non-empty fields are conditions.
func (eg *EngineGroup) Sum(bean interface{}, colName string) (float64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Sum(bean, colName)
}

// SumInt sum the records by some column. bean's non-empty fields are conditions.
func (eg *EngineGroup) SumInt(bean interface{}, colName string) (int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.SumInt(bean, colName)
}

// Sums sum the records by some columns. bean's non-empty fields are conditions.
func (eg *EngineGroup) Sums(bean interface{}, colNames ...string) ([]float64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Sums(bean, colNames...)
}

// SumsInt like Sums but return slice of int64 instead of float64.
func (eg *EngineGroup) SumsInt(bean interface{}, colNames ...string) ([]int64, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.SumsInt(bean, colNames...)
}

// ImportFile SQL DDL file
func (eg *EngineGroup) ImportFile(ddlPath string) ([]sql.Result, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.ImportFile(ddlPath)
}

// Import SQL DDL from io.Reader
func (eg *EngineGroup) Import(r io.Reader) ([]sql.Result, error) {
	session := eg.NewSession()
	defer session.Close()
	return session.Import(r)
}

// GetColumnMapper returns the column name mapper
func (eg *EngineGroup) GetColumnMapper() names.Mapper {
	return eg.tagParser.GetColumnMapper()
}

// GetTableMapper returns the table name mapper
func (eg *EngineGroup) GetTableMapper() names.Mapper {
	return eg.tagParser.GetTableMapper()
}

// GetTZLocation returns time zone of the application
func (eg *EngineGroup) GetTZLocation() *time.Location {
	return eg.TZLocation
}

// SetTZLocation sets time zone of the application
func (eg *EngineGroup) SetTZLocation(tz *time.Location) {
	eg.TZLocation = tz
}

// GetTZDatabase returns time zone of the database
func (eg *EngineGroup) GetTZDatabase() *time.Location {
	return eg.DatabaseTZ
}

// SetTZDatabase sets time zone of the database
func (eg *EngineGroup) SetTZDatabase(tz *time.Location) {
	eg.DatabaseTZ = tz
}

// SetSchema sets the schema of database
func (eg *EngineGroup) SetSchema(schema string) {
	eg.dialect.URI().SetSchema(schema)
}

// Unscoped always disable struct tag "deleted"
func (eg *EngineGroup) Unscoped() *Session {
	session := eg.NewSession()
	session.isAutoClose = true
	return session.Unscoped()
}

func (eg *EngineGroup) tbNameWithSchema(v string) string {
	return dialects.TableNameWithSchema(eg.dialect, v)
}

// SetDefaultContext set the default context
func (eg *EngineGroup) SetDefaultContext(ctx context.Context) {
	eg.defaultContext = ctx
}

// PingContext tests if database is alive
func (eg *EngineGroup) PingContext(ctx context.Context) error {
	session := eg.NewSession()
	defer session.Close()
	return session.PingContext(ctx)
}

// Transaction Execute sql wrapped in a transaction(abbr as tx), tx will automatic commit if no errors occurred
func (eg *EngineGroup) Transaction(f func(*Session) (interface{}, error)) (interface{}, error) {
	session := eg.NewSession()
	defer session.Close()

	if err := session.Begin(); err != nil {
		return nil, err
	}

	result, err := f(session)
	if err != nil {
		return result, err
	}

	if err := session.Commit(); err != nil {
		return result, err
	}

	return result, nil
}
