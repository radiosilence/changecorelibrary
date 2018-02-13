package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/core_sdk"
	"fmt"
	"database/sql"
	"strconv"
	"strings"
	"bitbucket.org/tokom_/linkcore"
	"github.com/Afternight/Catch"
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

func CreateChangeCore(request linkcore.CreateCommonRequest,db *sql.DB, origin string)(linkcore.CreateCommonResponse){
	//Create our response
	resp := request.GetNewResponseObject()

	//Create the log
	log := new(Catch.Log)

	//set fluids
	setErr := resp.SetObjectValues(request.GetValues())

	if setErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,setErr,true,request.GetCreateRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	//Create the object in the database
	res, dbErr := request.InsertIntoDatabase(db)
	if dbErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,dbErr,true,request.GetCreateRectifier(origin))
		resp.SetLog(*log)
		return resp
	}
	pkID, pkErr := res.LastInsertId()
	if pkErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,pkErr,true,request.GetCreateRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	//Set tokoms now that they exist in the database
	tempTokom := new(linkcore.Tokom)
	tempUser := request.GetUser()
	tempTokom.CID = tempUser.CID
	tempTokom.CreatedBy = tempUser.UID
	tempTokom.PrimaryKey = pkID
	tempTokom.CreatedAt = "Not Implemented"

	resp.SetObjectTokomValues(*tempTokom)

	//Get switchboard codes
	var stubbedSwitch []core_sdk.IntegrationCode //todo make this actually get from switchboard

	//compare and reset codes
	tempCodes := CreateCodesFromSources(request.GetCodes().Values,stubbedSwitch,request.GetCodes().Inverted)
	request.SetCodes(tempCodes)

	//create product in our integrations
	dO, _, deltaErr := request.EnactDelta()


	//check if there was an error in sending the delta request
	if deltaErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,false,request.GetInstallRectifier(*tempTokom,origin))
		resp.SetLog(*log)
		//the responses object values have already been set
		return resp
	}

	deltaCommonReq := dO.(*core_sdk.CommonDeltaResponse)
	log.MergeLogs(deltaCommonReq.Log)

	if deltaCommonReq.Log.Fatality { //check if there was a fatality on delta
		resp.SetLog(*log)
		return resp
	}

	//if we are here we assume delta enacted atleast partially and begin installing
	corrErr := AddCorrelations(tempTokom.PrimaryKey, deltaCommonReq.IDs,db,request.GetTableName())

	//A corr err in this case means a complete failure of correlation insertion
	if corrErr != nil {
		log.AddNewFailureFromError(500, core_sdk.ProductDomain,corrErr,false,request.GetLinkRectifier(*tempTokom,origin,deltaCommonReq.IDs))
	} else {
		resp.SetIDs(deltaCommonReq.IDs)
	}

	resp.SetLog(*log)
	return resp
}
