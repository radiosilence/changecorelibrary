package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/core_sdk"
	"fmt"
	"database/sql"
	"strconv"
	"strings"
	"bitbucket.org/tokom_/linkcore"
	"github.com/Afternight/Catch"
	"errors"
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

func CreateChangeCore(request linkcore.CreateRequest,db *sql.DB, origin string)(linkcore.CreateResponse){
	//Create our response
	response := request.ConstructNewResponseObject()
	resp := response.(linkcore.CreateResponse)

	//Create the log
	log := new(Catch.Log)


	//set fluids
	setErr := resp.GetObject().SetValues(request.GetValues())

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

	resp.GetObject().SetTokom(*tempTokom)

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


	deltaCommonReq := dO.(linkcore.DeltaResponse)
	log.MergeLogs(deltaCommonReq.GetLog())

	if deltaCommonReq.GetLog().Fatality { //check if there was a fatality on delta
		resp.SetLog(*log)
		return resp
	}

	//if we are here we assume delta enacted atleast partially and begin installing
	corrErr := AddCorrelations(tempTokom.PrimaryKey, deltaCommonReq.GetIDs(),db,request.GetObjectHandle())

	//A corr err in this case means a complete failure of correlation insertion
	if corrErr != nil {
		log.AddNewFailureFromError(500, core_sdk.ProductDomain,corrErr,false,request.GetLinkRectifier(*tempTokom,origin,deltaCommonReq.GetIDs()))
	} else {
		resp.GetObject().SetIDs(deltaCommonReq.GetIDs())
	}

	resp.SetLog(*log)
	return resp
}

func ModifyChangeCore(request linkcore.ModifyRequest, db *sql.DB, origin string) (linkcore.ModifyResponse){
	//important consideration for this log is that modify is idempotent
	//This means that we are more likely to simply say send the request again if there was any kind of failure
	//this is also a necessity as there is no direct delta modifier action like install and link
	log := new(Catch.Log)

	//Create response
	response := request.ConstructNewResponseObject()
	resp := response.(linkcore.ModifyResponse)

	totalUpdateStatement := request.GetStatementArray()

	if len(totalUpdateStatement) == 0 {
		log.AddNewFailureFromError(400,core_sdk.ProductDomain,errors.New("No values given to modify"),true,request.GetModifyRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	query := fmt.Sprintf("UPDATE OBJ_Products SET %s WHERE PK_OBJ = ?", strings.Join(totalUpdateStatement,","))
	//Exec the update
	_ , dbErr := db.Exec(query,request.GetPrimaryKey())

	//if the db failed knockout
	if dbErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,dbErr,true,request.GetModifyRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	//if no ID's are given, obtain them to then push the modified changes
	if request.GetIDs() == nil {
		newIDs, idErr := core_sdk.GetObjectCorrelations(db,strconv.FormatInt(request.GetPrimaryKey(),10),request.GetObjectHandle())
		if idErr != nil {
			//if we fail to get the ID's, we treat it as a non fatal failure since we have the full object but we must return
			//immediately as the rest of hte function relies on those ID's
			log.AddNewFailureFromError(500,core_sdk.ProductDomain,idErr,false,request.GetModifyRectifier(origin))
			resp.SetLog(*log)
			resp.SetObjectFromPK(db,request.GetPrimaryKey()) //note this function has the side effect of directly logging object errors
			return resp
		}
		request.SetIDs(newIDs)
	}

	//Get Switchboard ID's
	var stubbedSwitch []core_sdk.IntegrationCode //todo make this actually get from switchboard

	//Combine the two ID sets together
	request.SetIDs(CreateIDsFromSources(request.GetIDs(),stubbedSwitch))

	//Enact the changes externally
	//We don't care for codes because we assume that no errors means full execution
	//We don't care for status because any status errors are logged in the deltaLog along with their rectifier
	deltaReq, _, deltaErr := request.EnactDelta()

	if deltaErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,false, request.GetModifyRectifier(origin))
		resp.SetLog(*log)
		resp.SetObjectFromPK(db,request.GetPrimaryKey())
		return resp
	}

	deltaComReq := deltaReq.(linkcore.DeltaResponse)

	//merge logs capturing any failures
	log.MergeLogs(deltaComReq.GetLog())

	//check if we failed to execute the delta, if we did we can bypass the full rerun by passing all the info
	//we already know
	if deltaErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,false, request.GetModifyRectifier(core_sdk.DeltaDomain))
	}


	resp.SetLog(*log)
	resp.SetObjectFromPK(db,request.GetPrimaryKey())
	return resp
}

func DeleteChangeCore(request linkcore.DeleteRequest, db *sql.DB, origin string) (linkcore.DeleteResponse) {
	response := request.ConstructNewResponseObject()
	resp := response.(linkcore.DeleteResponse)
	log := new(Catch.Log)

	//if no ID's are given, obtain all of them from the database
	if request.GetIDs() == nil {
		newIDs, idErr := core_sdk.GetObjectCorrelations(db,strconv.FormatInt(request.GetPrimaryKey(),10),request.GetObjectHandle())
		if idErr != nil {
			//we can't delete from our db if there are any correlations existing
			//therefore if we can't get those correlations we knockout
			log.AddNewFailureFromError(500,core_sdk.ProductDomain,idErr,true,request.GetDeleteRectifier(origin))
			resp.SetLog(*log)
			resp.SetPayload(false)
			return resp
		}
		request.SetIDs(newIDs)
	}

	//Get Switchboard ID's
	var stubbedSwitch []core_sdk.IntegrationCode //todo make this actually get from switchboard

	//Combine the two ID sets together
	request.SetIDs(CreateIDsFromSources(request.GetIDs(),stubbedSwitch))

	//push request to delta
	deltaReq, _, deltaErr := request.EnactDelta()

	if deltaErr != nil || deltaReq == nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,true, request.GetDeleteRectifier(origin))
		resp.SetLog(*log)
		resp.SetPayload(false)
		return resp
	}

	deltaResp := deltaReq.(linkcore.DeltaResponse)
	log.MergeLogs(deltaResp.GetLog())

	if deltaResp.GetLog().Fatality {
		resp.SetLog(*log)
		resp.SetPayload(false)
		return resp
	}

	var deleteStatements []string

	if len(deltaResp.GetIDs()) > 0 {
		for _, val := range deltaResp.GetIDs() {
			stringVal := fmt.Sprintf("(extCode = '%s' and extID = '%s' and FK_OBJ = %d)",val.IntegrationCode,val.IntegrationID,request.GetPrimaryKey())
			deleteStatements = append(deleteStatements,stringVal)
		}

		corrDBQuery := fmt.Sprintf("DELETE FROM COR_Products where %s",strings.Join(deleteStatements," or "))

		_, corrErr := db.Exec(corrDBQuery)

		if corrErr != nil {
			log.AddNewFailureFromError(500,core_sdk.ProductDomain,corrErr,true, request.GetDeleteRectifier(origin))
			resp.SetLog(*log)
			resp.SetPayload(false)
			return resp
		}
	}

	_, dbErrCass := db.Exec("DELETE FROM OBJ_Products WHERE PK_OBJ = ?",strconv.FormatInt(request.GetPrimaryKey(), 10))

	if dbErrCass != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,false, request.GetDeleteRectifier(origin))
		resp.SetLog(*log)
		resp.SetPayload(false)
		return resp
	}
	resp.SetLog(*log)
	resp.SetPayload(true)
	return resp
}

func InstallChangeCore (request linkcore.InstallRequest, db *sql.DB, origin string) (linkcore.InstallResponse) {
	log := new(Catch.Log)
	resp := request.ConstructNewResponseObject().(linkcore.InstallResponse)

	//note that the codes and such are created according to what was sent
	createRequest, createErr := request.ConstructCreateRequest(db)
	if createErr != nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,createErr,true,request.GetInstallRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	//consider adding switchboard check here to allow null passing of codes, currently install requires

	//create product in our integrations
	dO, _, deltaErr := createRequest.EnactDelta()

	//check if there was an error in sending the delta request
	if deltaErr != nil || dO == nil {
		log.AddNewFailureFromError(500,core_sdk.ProductDomain,deltaErr,true,request.GetInstallRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	deltaComReq := dO.(linkcore.DeltaResponse)
	//merge our logs, collecting our delta failures
	log.MergeLogs(deltaComReq.GetLog())

	if deltaComReq.GetLog().Fatality { //check if there was a fatality on delta
		resp.SetLog(*log)
		return resp
	}

	//if we are here we assume delta enacted atleast partially and begin installing
	corrErr := AddCorrelations(request.GetPrimaryKey(), deltaComReq.GetIDs(),db,request.GetObjectHandle())

	//A corr err in this case means a complete failure of correlation insertion
	//link in itself is idempotent, so sending a rectifier over the top is only a performance problem
	//todo add function that checks on resps set object if it has gotten all ID's that were hit with delta, if not run in the link rectifier
	if corrErr != nil {
		log.AddNewFailureFromError(500, core_sdk.ProductDomain,corrErr,false,request.GetLinkRectifier(origin,deltaComReq.GetIDs()))
	}

	resp.SetObjectFromPK(db,request.GetPrimaryKey())
	resp.SetLog(*log)
	return resp
}

func LinkChangeCore(request linkcore.LinkRequest, db *sql.DB, origin string) (linkcore.LinkResponse){
	log := new(Catch.Log)
	resp := request.ConstructNewResponseObject().(linkcore.LinkResponse)
	corrErr := AddCorrelations(request.GetPrimaryKey(),request.GetIDs(),db,request.GetObjectHandle())

	if corrErr != nil {
		log.AddNewFailureFromError(500,origin,corrErr,true,request.GetLinkRectifier(origin))
		resp.SetLog(*log)
		return resp
	}

	setErr := resp.SetObjectFromPK(db,request.GetPrimaryKey())

	if setErr != nil {
		log.AddNewFailureFromError(500,origin,corrErr,false,request.GetLinkRectifier(origin))
	}
	return resp
}