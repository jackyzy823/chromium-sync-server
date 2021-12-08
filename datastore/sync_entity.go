package datastore

import (
	"database/sql"
	"fmt"
	"github.com/brave/go-sync/schema/protobuf/sync_pb"
	"github.com/brave/go-sync/utils"
	"github.com/mattn/go-sqlite3"
	"github.com/satori/go.uuid"
	"google.golang.org/protobuf/proto"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Datastore abstracts over the underlying datastore.
type Datastore interface {
	// Insert a new sync entity.
	InsertSyncEntity(entity *SyncEntity) (bool, error)
	// Insert a series of sync entities in a write transaction.
	InsertSyncEntitiesWithServerTags(entities []*SyncEntity) error
	// Update an existing sync entity.
	UpdateSyncEntity(entity *SyncEntity, oldVersion int64) (conflict bool, delete bool, err error)
	// Get updates for a specific type which are modified after the time of
	// client token for a given client. Besides the array of sync entities, a
	// boolean value indicating whether there are more updates to query in the
	// next batch is returned.
	GetUpdatesForType(dataType int, clientToken int64, fetchFolders bool, clientID string, maxSize int64) (bool, []SyncEntity, error)
	// Check if a server-defined unique tag is in the datastore.
	HasServerDefinedUniqueTag(clientID string, tag string) (bool, error)
	// Get the count of sync items for a client.
	GetClientItemCount(clientID string) (int, error)
	// Update the count of sync items for a client.
	UpdateClientItemCount(clientID string, count int) error
}

const (
	clientTagItemPrefix = "Client#"
	serverTagItemPrefix = "Server#"
)

type SyncEntity struct {
	ClientID               string
	ID                     string
	ParentID               *string
	Version                *int64
	Mtime                  *int64
	Ctime                  *int64
	Name                   *string
	NonUniqueName          *string
	ServerDefinedUniqueTag *string
	Deleted                *bool
	OriginatorCacheGUID    *string
	OriginatorClientItemID *string
	Specifics              []byte
	DataType               *int
	Folder                 *bool
	ClientDefinedUniqueTag *string
	UniquePosition         []byte
}

// var sql_CreateTable = `
//     create table if not exists SyncEntity(
//         ClientID TEXT NOT NULL,
//         ID TEXT NOT NULL,
//         ParentID TEXT,
//         Version INT NOT NULL,
//         Mtime INT NOT NULL,
//         Ctime INT NOT NULL,
//         Name TEXT,
//         NonUniqueName TEXT,
//         ServerDefinedUniqueTag TEXT,
//         Deleted BOOL NOT NULL,
//         OriginatorCacheGUID TEXT,
//         OriginatorClientItemID TEXT,
//         Specifics BLOB NOT NULL,
//         DataType INT NOT NULL,
//         Folder BOOL NOT NULL,
//         ClientDefinedUniqueTag TEXT,
//         UniquePosition BLOB
//         DataTypeMtime TEXT
//         );
//     `
var sql_CreateTable = `
    CREATE TABLE IF NOT EXISTS SyncEntity(
        ClientID TEXT NOT NULL,
        ID TEXT NOT NULL,
        ParentID TEXT,
        Version INT ,
        Mtime INT ,
        Ctime INT ,
        Name TEXT,
        NonUniqueName TEXT,
        ServerDefinedUniqueTag TEXT,
        Deleted BOOL ,
        OriginatorCacheGUID TEXT,
        OriginatorClientItemID TEXT,
        Specifics BLOB ,
        DataType INT ,
        Folder BOOL ,
        ClientDefinedUniqueTag TEXT,
        UniquePosition BLOB,
        UNIQUE(ClientID , ID)
        );
    `

//for InsertSyncEntitiesWithServerTags
var sql_Insert_tag_item = `INSERT INTO SyncEntity (ClientID , ID) VALUES (? , ?); `

// primary key ?? unique ?? index ??
type LiteDB struct {
	*sql.DB
}

func NewLiteDB(path string) (*LiteDB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(sql_CreateTable)
	if err != nil {
		db.Close()
		return nil, err
	}

	return &LiteDB{db}, nil
}

// NewServerClientUniqueTagItem creates a tag item which is used to ensure the
// uniqueness of server-defined or client-defined unique tags for a client.
// func NewServerClientUniqueTagItem(clientID string, tag string, isServer bool) *ServerClientUniqueTagItem {
// 	prefix := clientTagItemPrefix
// 	if isServer {
// 		prefix = serverTagItemPrefix
// 	}
//
// 	return &ServerClientUniqueTagItem{
// 		ClientID: clientID,
// 		ID:       prefix + tag,
// 	}
// }

func TagToID(tag string, isServer bool) string {
	prefix := clientTagItemPrefix
	if isServer {
		prefix = serverTagItemPrefix
	}
	return prefix + tag
}

func checkConflictError(err error) bool {
	if serr, ok := err.(sqlite3.Error); ok && (serr.ExtendedCode == sqlite3.ErrConstraintUnique || serr.ExtendedCode == sqlite3.ErrConstraintPrimaryKey) {
		return true
	}
	return false
}

// for InsertSyncEntitiesWithServerTags and  InsertSyncEntity
var sql_Insert_sync_item = `INSERT INTO SyncEntity (ClientID, ID, ParentID , Version,  Mtime , Ctime, Name , NonUniqueName ,  ServerDefinedUniqueTag , Deleted, OriginatorCacheGUID ,  OriginatorClientItemID , Specifics , DataType, Folder,  ClientDefinedUniqueTag , UniquePosition ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ); `

func doInsert(tx *sql.Tx, entity *SyncEntity) (sql.Result, error) {
	return tx.Exec(sql_Insert_sync_item, entity.ClientID, entity.ID,
		entity.ParentID, entity.Version, entity.Mtime, entity.Ctime, entity.Name, entity.NonUniqueName, entity.ServerDefinedUniqueTag, entity.Deleted, entity.OriginatorCacheGUID, entity.OriginatorClientItemID, entity.Specifics, entity.DataType, entity.Folder, entity.ClientDefinedUniqueTag, entity.UniquePosition)
}

var sql_HasServerDefinedUniqueTag = `SELECT COUNT(ClientID) FROM SyncEntity WHERE ClientID  = ? and ID = ?;`

func (db *LiteDB) HasServerDefinedUniqueTag(clientID string, tag string) (bool, error) {
	var result int
	err := db.QueryRow(sql_HasServerDefinedUniqueTag, clientID, TagToID(tag, true)).Scan(&result)
	if err != nil {
		return false, err
	}
	return result != 0, nil
}

var sql_GetClientItemCount = `SELECT COUNT(ClientID) FROM SyncEntity WHERE ClientID = ?;`

func (db *LiteDB) GetClientItemCount(clientID string) (int, error) {
	var result int
	err := db.QueryRow(sql_GetClientItemCount, clientID).Scan(&result)
	if err != nil {
		return 0, err
	}
	return result, nil
}

// NOTE We do not need update .
func (db *LiteDB) UpdateClientItemCount(clientID string, count int) error {
	return nil
}

// bool conflict?
func (db *LiteDB) InsertSyncEntity(entity *SyncEntity) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	if entity.ClientDefinedUniqueTag != nil {
		_, err = tx.Exec(sql_Insert_tag_item, entity.ClientID, TagToID(*entity.ClientDefinedUniqueTag, false))
		if err != nil {
			tx.Rollback()
			return checkConflictError(err), fmt.Errorf("error writing sync entities with server tags in a transaction: %w", err)
		}
	}

	_, err = doInsert(tx, entity)
	if err != nil {
		tx.Rollback()
		return checkConflictError(err), fmt.Errorf("error writing sync entities with server tags in a transaction: %w", err)
	}

	tx.Commit()
	return false, nil
}

var sql_UpdateSyncEntity_start = `UPDATE SyncEntity SET Version=@version , Mtime=@mtime , Specifics=@specifics `
var sql_UpdateSyncEntity_UniquePosition = ` , UniquePosition=@uniqueposition `
var sql_UpdateSyncEntity_ParentID = ` , ParentID=@parentid `
var sql_UpdateSyncEntity_Name = ` , Name=@name `
var sql_UpdateSyncEntity_NonUniqueName = ` , NonUniqueName=@nonuniquename `
var sql_UpdateSyncEntity_Deleted = ` , Deleted=@deleted `
var sql_UpdateSyncEntity_Folder = ` , Folder=@folder `
var sql_UpdateSyncEntity_end = ` WHERE ClientID = @clientid AND ID = @id AND Version = @oldVersion;`

var sql_Delete_tag_item = `DELETE FROM SyncEntity WHERE ClientID = ? AND ID = ?; `

// NOTE we do not use UpdateClientItemCount at all so `delete` do no effects
func (db *LiteDB) UpdateSyncEntity(entity *SyncEntity, oldVersion int64) (conflict bool, delete bool, err error) {
	var args []interface{}
	args = append(args, sql.Named("oldVersion", oldVersion),
		sql.Named("clientid", entity.ClientID),
		sql.Named("id", entity.ID),
		sql.Named("version", entity.Version),
		sql.Named("mtime", entity.Mtime),
		sql.Named("specifics", entity.Specifics))

	sql_UpdateSyncEntity := sql_UpdateSyncEntity_start
	if entity.UniquePosition != nil {
		sql_UpdateSyncEntity += sql_UpdateSyncEntity_UniquePosition
		args = append(args, sql.Named("uniqueposition", entity.UniquePosition))
	}

	if entity.ParentID != nil {
		sql_UpdateSyncEntity += sql_UpdateSyncEntity_ParentID
		args = append(args, sql.Named("parentid", entity.ParentID))
	}

	if entity.Name != nil {
		sql_UpdateSyncEntity += sql_UpdateSyncEntity_Name
		args = append(args, sql.Named("name", entity.Name))
	}

	if entity.NonUniqueName != nil {
		sql_UpdateSyncEntity += sql_UpdateSyncEntity_NonUniqueName
		args = append(args, sql.Named("nonuniquename", entity.NonUniqueName))
	}

	if entity.Deleted != nil {
		sql_UpdateSyncEntity += sql_UpdateSyncEntity_Deleted
		args = append(args, sql.Named("deleted", entity.Deleted))
	}

	if entity.Folder != nil {
		sql_UpdateSyncEntity += sql_UpdateSyncEntity_Folder
		args = append(args, sql.Named("folder", entity.Folder))
	}

	sql_UpdateSyncEntity += sql_UpdateSyncEntity_end

	tx, err := db.Begin()
	if err != nil {
		return false, false, err
	}

	_, err = tx.Exec(sql_UpdateSyncEntity, args...)

	if err != nil {
		tx.Rollback()
		return checkConflictError(err), false, fmt.Errorf("Error in UpdateSyncEntity: %w", err)
	}

	if entity.Deleted != nil && entity.ClientDefinedUniqueTag != nil && *entity.Deleted {
		_, err = tx.Exec(sql_Delete_tag_item, entity.ClientID, TagToID(*entity.ClientDefinedUniqueTag, false))
		if err != nil {
			tx.Rollback()
			return checkConflictError(err), false, fmt.Errorf("Error in UpdateSyncEntity Delete tag item: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return false, false, fmt.Errorf("error UpdateSyncEntity in a commit: %w", err)
	}
	// //TODO althoug delete is not important//
	return false, (!(entity.Deleted == nil)) && *entity.Deleted, nil
}

// GetUpdatesForType returns sync entities of a data type where it's mtime is
// later than the client token.

// we ignore maxSize!
var sql_GetUpdatesForType_Without_Folders = `SELECT ClientID, ID, ParentID , Version,  Mtime , Ctime, Name , NonUniqueName ,  ServerDefinedUniqueTag , Deleted, OriginatorCacheGUID ,  OriginatorClientItemID , Specifics , DataType, Folder,  ClientDefinedUniqueTag , UniquePosition  FROM SyncEntity WHERE Folder = FALSE AND ClientID = ? AND DataType=? AND Mtime > ? ORDER BY Mtime ASC;`
var sql_GetUpdatesForType = `SELECT ClientID, ID, ParentID , Version,  Mtime , Ctime, Name , NonUniqueName ,  ServerDefinedUniqueTag , Deleted, OriginatorCacheGUID ,  OriginatorClientItemID , Specifics , DataType, Folder,  ClientDefinedUniqueTag , UniquePosition  FROM SyncEntity WHERE ClientID = ? AND DataType=? AND Mtime > ? ORDER BY Mtime ASC;`

func (db *LiteDB) GetUpdatesForType(dataType int, clientToken int64, fetchFolders bool, clientID string, maxSize int64) (bool, []SyncEntity, error) { //haschangingremain??
	syncEntities := []SyncEntity{}

	stmt := sql_GetUpdatesForType_Without_Folders
	if fetchFolders {
		stmt = sql_GetUpdatesForType
	}
	rows, err := db.Query(stmt, clientID, dataType, clientToken)
	if err != nil {
		return false, nil, err
	}
	defer rows.Close()
	var entity SyncEntity
	for rows.Next() {
		rows.Scan(&entity.ClientID, &entity.ID, &entity.ParentID, &entity.Version, &entity.Mtime, &entity.Ctime, &entity.Name, &entity.NonUniqueName, &entity.ServerDefinedUniqueTag, &entity.Deleted, &entity.OriginatorCacheGUID, &entity.OriginatorClientItemID, &entity.Specifics, &entity.DataType, &entity.Folder, &entity.ClientDefinedUniqueTag, &entity.UniquePosition)
		syncEntities = append(syncEntities, entity)
	}

	return false, syncEntities, nil

}

// InsertSyncEntitiesWithServerTags is used to insert sync entities with
// server-defined unique tags. To ensure the uniqueness, for each sync entity,
// we will write a tag item and a sync item. Items for all the entities in the
// array would be written into DB in one transaction.
func (db *LiteDB) InsertSyncEntitiesWithServerTags(entities []*SyncEntity) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, entity := range entities {
		_, err = tx.Exec(sql_Insert_tag_item, entity.ClientID, TagToID(*entity.ServerDefinedUniqueTag, true))
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error writing sync entities with server tags in a transaction: %w", err)
		}
		_, err = doInsert(tx, entity)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error writing sync entities with server tags in a transaction: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error writing sync entities with server tags in a transaction: %w", err)
	}
	return nil
}

func validatePBEntity(entity *sync_pb.SyncEntity) error {
	if entity == nil {
		return fmt.Errorf("validate SyncEntity error: empty SyncEntity")
	}

	if entity.IdString == nil {
		return fmt.Errorf("validate SyncEntity error: empty IdString")
	}

	if entity.Version == nil {
		return fmt.Errorf("validate SyncEntity error: empty Version")
	}

	if entity.Specifics == nil {
		return fmt.Errorf("validate SyncEntity error: nil Specifics")
	}

	return nil
}

func CreateDBSyncEntity(entity *sync_pb.SyncEntity, cacheGUID *string, clientID string) (*SyncEntity, error) {
	err := validatePBEntity(entity)
	if err != nil {
		//log.Error().Err(err).Msg("Invalid sync_pb.SyncEntity received")
		return nil, fmt.Errorf("error validating protobuf sync entity to create DB sync entity: %w", err)
	}

	// Specifics are always passed and checked by validatePBEntity above.
	var specifics []byte
	specifics, err = proto.Marshal(entity.Specifics)
	if err != nil {
		//log.Error().Err(err).Msg("Marshal specifics failed")
		return nil, fmt.Errorf("error marshalling specifics to create DB sync entity: %w", err)
	}

	// Use reflect to find out data type ID defined in protobuf tag.
	structField := reflect.ValueOf(entity.Specifics.SpecificsVariant).Elem().Type().Field(0)
	tag := structField.Tag.Get("protobuf")
	s := strings.Split(tag, ",")
	dataType, _ := strconv.Atoi(s[1])

	var uniquePosition []byte
	if entity.UniquePosition != nil {
		uniquePosition, err = proto.Marshal(entity.UniquePosition)
		if err != nil {
			//log.Error().Err(err).Msg("Marshal UniquePosition failed")
			return nil, fmt.Errorf("error marshalling unique position to create DB sync entity: %w", err)
		}
	}

	id := *entity.IdString
	var originatorCacheGUID, originatorClientItemID *string
	if cacheGUID != nil {
		if *entity.Version == 0 {
			id = uuid.NewV4().String()
		}
		originatorCacheGUID = cacheGUID
		originatorClientItemID = entity.IdString
	}

	now := func(arg int64) *int64 { return &arg }(utils.UnixMilli(time.Now()))
	// ctime is only used when inserting a new entity, here we use client passed
	// ctime if it is passed, otherwise, use current server time as the creation
	// time. When updating, ctime will be ignored later in the query statement.
	cTime := now
	if entity.Ctime != nil {
		cTime = entity.Ctime
	}

	//dataTypeMtime := strconv.Itoa(dataType) + "#" + strconv.FormatInt(now, 10)

	// Set default values on Deleted and Folder attributes for new entities, the
	// default values are specified by sync.proto protocol.
	deleted := entity.Deleted
	folder := entity.Folder
	if *entity.Version == 0 {
		if entity.Deleted == nil {
			deleted = func(b bool) *bool { return &b }(false)
		}
		if entity.Folder == nil {
			folder = func(b bool) *bool { return &b }(false)
		}
	}

	return &SyncEntity{
		ClientID:               clientID,
		ID:                     id,
		ParentID:               entity.ParentIdString,
		Version:                entity.Version,
		Ctime:                  cTime,
		Mtime:                  now,
		Name:                   entity.Name,
		NonUniqueName:          entity.NonUniqueName,
		ServerDefinedUniqueTag: entity.ServerDefinedUniqueTag,
		Deleted:                deleted,
		OriginatorCacheGUID:    originatorCacheGUID,
		OriginatorClientItemID: originatorClientItemID,
		ClientDefinedUniqueTag: entity.ClientDefinedUniqueTag,
		Specifics:              specifics,
		Folder:                 folder,
		UniquePosition:         uniquePosition,
		DataType:               func(b int) *int { return &b }(dataType),
	}, nil
}

// CreatePBSyncEntity converts a DB sync item to a protobuf sync entity.
func CreatePBSyncEntity(entity *SyncEntity) (*sync_pb.SyncEntity, error) {
	pbEntity := &sync_pb.SyncEntity{
		IdString:               &entity.ID,
		ParentIdString:         entity.ParentID,
		Version:                entity.Version,
		Mtime:                  entity.Mtime,
		Ctime:                  entity.Ctime,
		Name:                   entity.Name,
		NonUniqueName:          entity.NonUniqueName,
		ServerDefinedUniqueTag: entity.ServerDefinedUniqueTag,
		ClientDefinedUniqueTag: entity.ClientDefinedUniqueTag,
		OriginatorCacheGuid:    entity.OriginatorCacheGUID,
		OriginatorClientItemId: entity.OriginatorClientItemID,
		Deleted:                entity.Deleted,
		Folder:                 entity.Folder,
	}

	if entity.Specifics != nil {
		pbEntity.Specifics = &sync_pb.EntitySpecifics{}
		err := proto.Unmarshal(entity.Specifics, pbEntity.Specifics)
		if err != nil {
			//log.Error().Err(err).Msg("Unmarshal specifics failed")
			return nil, fmt.Errorf("error unmarshalling specifics to create protobuf sync entity: %w", err)
		}
	}

	if entity.UniquePosition != nil {
		pbEntity.UniquePosition = &sync_pb.UniquePosition{}
		err := proto.Unmarshal(entity.UniquePosition, pbEntity.UniquePosition)
		if err != nil {
			//log.Error().Err(err).Msg("Unmarshal UniquePosition failed")
			return nil, fmt.Errorf("error unmarshalling unique position to create protobuf sync entity: %w", err)
		}
	}

	return pbEntity, nil
}
