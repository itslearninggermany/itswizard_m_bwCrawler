package main

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/itslearninggermany/itswizard_m_awsbrooker"
	"github.com/itslearninggermany/itswizard_m_basic"
	"github.com/itslearninggermany/itswizard_m_bwStructs"
	"github.com/itslearninggermany/itswizard_m_imses"
	"github.com/jinzhu/gorm"
	"log"
	"strconv"
)

func mainprogramm(logger *logger) {
	logger.log(nil, "BW Crawler", "Start")
	// Datenbank einrichten
	var databaseConfig []itswizard_m_basic.DatabaseConfig
	b, _ := itswizard_m_awsbrooker.DownloadFileFromBucket("brooker", "admin/databaseconfig.json")
	err := json.Unmarshal(b, &databaseConfig)
	if err != nil {
		log.Println(err)
	}
	allDatabases := make(map[string]*gorm.DB)
	for i := 0; i < len(databaseConfig); i++ {
		database, err := gorm.Open(databaseConfig[i].Dialect, databaseConfig[i].Username+":"+databaseConfig[i].Password+"@tcp("+databaseConfig[i].Host+")/"+databaseConfig[i].NameOrCID+"?charset=utf8&parseTime=True&loc=Local")
		if err != nil {
			log.Println(err)
		}
		allDatabases[databaseConfig[i].NameOrCID] = database
	}
	log.Println("All Databases loaded")

	var services []itswizard_m_basic.Service
	err = allDatabases["Client"].Where("bw_admin = ?", true).Find(&services).Error
	if err != nil {
		fmt.Println(err)
		logger.log(err, "", "Getting Service")
	}
	fmt.Println(services)
	logger.log(nil, "Services", fmt.Sprint(services))

	//	var firstRun bool
	//	var lastRun time.Time
	//	firstRun = true

	/*
		//nur einmal pro Stunde
		log.Println(time.Now().Sub(lastRun).Hours())
		if firstRun || (time.Now().Sub(lastRun) > 0){
			lastRun = time.Now()
			err = allDatabases["Client"].Where("bw_admin = ?", true).Find(&services).Error
			if err != nil {
				log.Println(err)
			}
			firstRun = false
			log.Println(services)
		}
	*/

	for _, service := range services {
		if service.InstitutionID != 51 {
			continue
		}
		log.Println("hier!")
		database := allDatabases[strconv.Itoa(int(service.InstitutionID))]
		var imseses []itswizard_m_basic.ImsesSetup
		err = database.Find(&imseses).Error
		if err != nil {
			log.Println(err)
			logger.log(err, "Get IMSES Cred", "")
		}
		imsesServices := make(map[uint]*itswizard_m_imses.Request)

		for _, i := range imseses {
			imsesServices[i.OrganisationID] = itswizard_m_imses.NewImsesService(itswizard_m_imses.NewImsesServiceInput{
				Username: i.Username,
				Password: i.Password,
				Url:      i.Endpoint,
			})
		}

		var err error
		var psmToImport []itswizard_m_bwStructs.BWPersonSchoolMembership
		var psmToUpdate []itswizard_m_bwStructs.BWPersonSchoolMembership
		var psmToDelete []itswizard_m_bwStructs.BWPersonSchoolMembership
		var pcmToImport []itswizard_m_bwStructs.BWPersonClassMembership
		var pcmToUpdate []itswizard_m_bwStructs.BWPersonClassMembership
		var pcmToDelete []itswizard_m_bwStructs.BWPersonClassMembership
		var classesToImport []itswizard_m_bwStructs.BWClass
		var classesToUpdate []itswizard_m_bwStructs.BWClass
		var classesToDelete []itswizard_m_bwStructs.BWClass
		var schoolsToImport []itswizard_m_bwStructs.BWSchool
		var schoolsToUpdate []itswizard_m_bwStructs.BWSchool
		var schoolsToDelete []itswizard_m_bwStructs.BWSchool
		var personsToImport []itswizard_m_bwStructs.BWPerson
		var personsToUpdate []itswizard_m_bwStructs.BWPerson
		var personsToDelete []itswizard_m_bwStructs.BWPerson

		psmToImport, psmToUpdate, psmToDelete, err = itswizard_m_bwStructs.GetAllPsmToImportAndPsmToUpdateAndPsmToDelete(database)
		pcmToImport, pcmToUpdate, pcmToDelete, err = itswizard_m_bwStructs.GetAllPcmToImportAndPcmToUpdateAndPcmToDelete(database)
		classesToImport, classesToUpdate, classesToDelete, err = itswizard_m_bwStructs.GetAllClassesToImortAndToUpdateAndToDelete(database)
		schoolsToImport, schoolsToUpdate, schoolsToDelete, err = itswizard_m_bwStructs.GetAllSchoolsToImportAndToUpdateAndToImport(database)
		personsToImport, personsToUpdate, personsToDelete, err = itswizard_m_bwStructs.GetAllPerosnsToUpdateAndImportAndDelete(database)
		if err != nil {
			continue
		}
		school(schoolsToImport, schoolsToUpdate, schoolsToDelete, imsesServices, imsesServices[0], database, logger)
		class(classesToImport, classesToUpdate, classesToDelete, imsesServices, database, logger)
		person(personsToImport, personsToUpdate, personsToDelete, imsesServices, imsesServices[0], database, logger)
		psm(psmToImport, psmToUpdate, psmToDelete, imsesServices, imsesServices[0], database, logger)
		pcm(pcmToImport, pcmToUpdate, pcmToDelete, imsesServices, database, logger)
	}

	finishedMainRoutine = true
}

func pcm(importData []itswizard_m_bwStructs.BWPersonClassMembership, updateData []itswizard_m_bwStructs.BWPersonClassMembership, deleteData []itswizard_m_bwStructs.BWPersonClassMembership, cluster map[uint]*itswizard_m_imses.Request, db *gorm.DB, logger *logger) {
	for _, s := range importData {
		s.Import(cluster[s.OrganisationID], db)
		logger.log(nil, "Import ClassMembership", fmt.Sprint(s))
	}
	for _, s := range updateData {
		s.Update(cluster[s.OrganisationID], db)
		logger.log(nil, "Update ClassMembership", fmt.Sprint(s))
	}
	for _, s := range deleteData {
		s.Delete(cluster[s.OrganisationID], db)
		logger.log(nil, "Delete ClassMemebership", fmt.Sprint(s))
	}
}

func psm(psmToImport []itswizard_m_bwStructs.BWPersonSchoolMembership, psmToUpdate []itswizard_m_bwStructs.BWPersonSchoolMembership, psmToDelete []itswizard_m_bwStructs.BWPersonSchoolMembership, cluster map[uint]*itswizard_m_imses.Request, specialCluster *itswizard_m_imses.Request, db *gorm.DB, logger *logger) {
	for _, s := range psmToImport {
		s.Import(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Import SchoolMemebership", fmt.Sprint(s))
	}
	for _, s := range psmToUpdate {
		s.Update(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Update SchoolMemebership", fmt.Sprint(s))

	}
	for _, s := range psmToDelete {
		s.Delete(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Delete SchoolMemebership", fmt.Sprint(s))

	}
}

func person(personsToImport []itswizard_m_bwStructs.BWPerson, personsToUpdate []itswizard_m_bwStructs.BWPerson, personsToDelete []itswizard_m_bwStructs.BWPerson, cluster map[uint]*itswizard_m_imses.Request, specialCluster *itswizard_m_imses.Request, db *gorm.DB, logger *logger) {
	for _, s := range personsToImport {
		s.Import(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Import Person", fmt.Sprint(s))
	}
	for _, s := range personsToUpdate {
		s.Update(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Update Person", fmt.Sprint(s))

	}
	for _, s := range personsToDelete {
		s.Delete(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Delete Person", fmt.Sprint(s))

	}
}

func school(schoolsToImport []itswizard_m_bwStructs.BWSchool, schoolsToUpdate []itswizard_m_bwStructs.BWSchool, schoolsToDelete []itswizard_m_bwStructs.BWSchool, cluster map[uint]*itswizard_m_imses.Request, specialCluster *itswizard_m_imses.Request, db *gorm.DB, logger *logger) {
	for _, s := range schoolsToImport {
		s.Import(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Import School", fmt.Sprint(s))

	}
	for _, s := range schoolsToUpdate {
		s.Update(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Uopdate School", fmt.Sprint(s))

	}
	for _, s := range schoolsToDelete {
		s.Delete(specialCluster, cluster[s.OrganisationID], db)
		logger.log(nil, "Delete School", fmt.Sprint(s))

	}
}

func class(classsesToImport []itswizard_m_bwStructs.BWClass, classesToUpdate []itswizard_m_bwStructs.BWClass, classesToDelete []itswizard_m_bwStructs.BWClass, cluster map[uint]*itswizard_m_imses.Request, db *gorm.DB, logger *logger) {
	fmt.Println(classsesToImport)
	for _, s := range classsesToImport {
		s.Import(cluster[s.OrganisationID], db)
		logger.log(nil, "Import Class", fmt.Sprint(s))
	}
	for _, s := range classesToUpdate {
		s.Update(cluster[s.OrganisationID], db)
		logger.log(nil, "Update Person", fmt.Sprint(s))

	}
	for _, s := range classesToDelete {
		s.Delete(cluster[s.OrganisationID], db)
		logger.log(nil, "Delete Person", fmt.Sprint(s))

	}
}
