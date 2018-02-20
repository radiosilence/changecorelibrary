package ChangeCoreLibrary

import (
	"bitbucket.org/tokom_/linkcore"
	"database/sql"
	"github.com/Afternight/Catch"
	"bitbucket.org/tokom_/core_sdk"
)

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

