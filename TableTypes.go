package ChangeCoreLibrary

import (
	"database/sql"
	"fmt"
	"bitbucket.org/tokom_/linkcore"
)

//Supplementary table types (data storage mechanisms in the sql database typically)
//These table types are used in conjunction with standard sql to run in the actual change cores
//Essentially a library of common SQL operations that we streamline to common situations

// region Properties table

//PROP Table
//Prefixed with PROP_<tablename>
// For n..n key connections between standard flow objects
//ie a product has multiple categories, and a category has multiple products
//Columns
//PK_<tablename> - Indexer for the table, not used, %s = table name
//FK_PROP - FK to the foreign tokom object in another db, represented on a code level with the FK struct
//FK_OBJ - Fk to the local tokom object

//All operations on PROP tables base themselves on the FK_OBJ table
//Common constraint of unique FK_PROP, FK_OBJ

func GetProperties (db *sql.DB, objPk int64, propertyName string, propertyHandle string) ([]linkcore.FK, error) {
	queryString := fmt.Sprintf("SELECT FK_PROP from PROP_%s where FK_OBJ = ?",propertyName)
	res, dbErr := db.Query(queryString,objPk)
	if dbErr != nil {
		return nil, dbErr
	}
	var newFKs []linkcore.FK
	for res.Next() {
		curr := new(linkcore.FK)
		res.Scan(&curr.Key)
		curr.Identifiers.DatabaseHandle = propertyHandle
		newFKs = append(newFKs,*curr)
	}

	return newFKs,nil
}

func CreateProperties(db *sql.DB, objPk int64, fks []linkcore.FK, propertyName string) error {
	for _,val := range fks {
		if val.GetKey() > 0 {
			insertErr := CreateProperty(db,objPk,val,propertyName)
			if insertErr != nil {
				return insertErr
			}
		}
	}
	return nil
}

func CreateProperty(db *sql.DB, objPk int64, fk linkcore.FK, propertyName string) error {
	queryString := fmt.Sprintf("INSERT INTO PROP_%s (FK_PROP, FK_OBJ) VALUES (?,?)",propertyName)
	_, dbErr := db.Exec(queryString,fk.GetKey(),objPk)
	return dbErr
}

func DeleteAllProperties(db *sql.DB, objPk int64, propertyName string) error {
	queryString := fmt.Sprintf("DELETE FROM PROP_%s WHERE FK_OBJ = ?")
	_, dbErr := db.Exec(queryString)
	return dbErr
}

func UpdateProperties(db *sql.DB, objPk int64, newFks []linkcore.FK,propertyName string) error {
	//for now we are going to use a simplified version of delete all, insert all
	//as opposed to check all delete some insert some
	if len(newFks) == 0 {
		return nil //we are encapsulating the update logic here
	}

	delErr := DeleteAllProperties(db,objPk,propertyName)
	if delErr != nil {
		return delErr
	}

	return CreateProperties(db,objPk,newFks,propertyName)
}
// endregion