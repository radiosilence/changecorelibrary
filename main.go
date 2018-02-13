package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/core_sdk"
	"fmt"
	"database/sql"
	"strconv"
	"strings"
)

func CreateCodesFromSources(codes []core_sdk.IntegrationCode, switchboard []core_sdk.IntegrationCode, inverted bool) core_sdk.IntCodes {
	completeCodes := core_sdk.IntCodes{}
	if len(codes) == 0 {
		completeCodes.Values = switchboard
	} else {
		completeCodes.Values = codes
	}
	completeCodes.Inverted = inverted
	return completeCodes
}

func CreateIDsFromSources(ids []core_sdk.ExtID, switchboard []core_sdk.IntegrationCode) ([]core_sdk.ExtID){
	if len(switchboard) == 0 {
		return ids
	} else {
		var NewIdSet []core_sdk.ExtID

		switchMap := make(map[string]bool)

		for _, val := range switchboard {
			switchMap[string(val)] = true
		}

		//return the overlap
		for _, val := range ids {
			if switchMap[string(val.IntegrationCode)] {
				NewIdSet = append(NewIdSet, val)
			}
		}
		return NewIdSet
	}
}

func GetObjectCorrelations(db *sql.DB, fk string, tableName string) ([]core_sdk.ExtID, error){
	queryString := fmt.Sprintf("Select extCode, extID from COR_%s where FK_OBJ = ?",tableName)
	dbRes, dbErr := db.Query(queryString,fk)
	if dbErr != nil {
		return nil, dbErr
	}

	var newIDs []core_sdk.ExtID
	for dbRes.Next() {
		var currID core_sdk.ExtID
		intCode := new(string)
		scanErr := dbRes.Scan(&intCode, &currID.IntegrationID)
		currID.IntegrationCode = core_sdk.IntegrationCode(*intCode)
		if scanErr != nil {
			continue
		}
		newIDs = append(newIDs, currID)
	}
	return newIDs,nil
}

func AddCorrelations(pk int64, newIDs []core_sdk.ExtID, db *sql.DB, tableName string) error {
	var insertStatements []string
	for _, extID := range newIDs {
		stringVal := fmt.Sprintf("('%s','%s',%s)",extID.IntegrationCode,extID.IntegrationID,strconv.FormatInt(pk,10))
		insertStatements = append(insertStatements,stringVal)
	}

	if len(insertStatements) > 0 {
		corrDBQuery := fmt.Sprintf("insert into COR_%s (extCode,extID,FK_OBJ) values %s", tableName, strings.Join(insertStatements, ","))
		_, dbExecErr := db.Exec(corrDBQuery)
		if dbExecErr != nil {
			return dbExecErr
		}
	}

	return nil
}
