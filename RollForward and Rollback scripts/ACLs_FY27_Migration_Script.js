// Load the logger
load('/utils/logger.js');
const log = getLogger();

// Connect to database
db = db.getSiblingDB('isqc');

log.info("Starting FY2027 migration operations");

// Track inserted IDs for rollback
const insertedAccessControlIds = [];
const insertedEnumerationIds = [];

try {
    // Add ACLS FY2027
    log.info("Adding Access Control entries for FY2027");

    db.accesscontrol.find({roleId: {$regex:".*_2026"}}).forEach(function (rta) {
        prevYear = '2026';
        currentYear = `2027`;

        rta.roleId = rta.roleId.replace(prevYear, currentYear);
        rta.uri = rta.uri.replace(prevYear, currentYear);

        const result = db.accesscontrol.insertOne({ 
            permission: rta.permission, 
            roleId: rta.roleId, 
            uri: rta.uri, 
            order: db.accesscontrol.find().sort({ '_id': -1 }).limit(1).toArray()[0].order + 1 
        });

        insertedAccessControlIds.push(result.insertedId);
    });

    log.logOp("Access Control entries added", { count: insertedAccessControlIds.length });

    // Add Enumeration FY2027
    log.info("Adding Enumeration entries for FY2027");

    db.enumeration.find({type:'RoleType',_id: {$regex:".*_2026"}, isRetired:false}).forEach(function (rta) {
        prevYear = '2026';
        currentYear = `2027`;

        rta._id = rta._id.replace(prevYear, currentYear);
        const result = db.enumeration.insertOne({ 
            _id: rta._id, 
            type: rta.type, 
            isRetired: rta.isRetired, 
            fiscalYear: [ 2023, 2024, 2025, 2026, 2027 ], 
            order: db.accesscontrol.find().sort({ '_id': -1 }).limit(1).toArray()[0].order + 1 
        });

        insertedEnumerationIds.push(rta._id);
    });

    log.logOp("Enumeration entries added", { count: insertedEnumerationIds.length });
    log.info("FY2027 migration completed successfully");

} catch (err) {
    log.error("FY2027 migration failed", err);
    log.info("Starting rollback operation");

    try {
        // Rollback: Delete inserted Access Control entries
        if (insertedAccessControlIds.length > 0) {
            const aclDeleteResult = db.accesscontrol.deleteMany({ _id: { $in: insertedAccessControlIds } });
            log.logOp("Rolled back Access Control entries", { deletedCount: aclDeleteResult.deletedCount });
        }

        // Rollback: Delete inserted Enumeration entries
        if (insertedEnumerationIds.length > 0) {
            const enumDeleteResult = db.enumeration.deleteMany({ _id: { $in: insertedEnumerationIds } });
            log.logOp("Rolled back Enumeration entries", { deletedCount: enumDeleteResult.deletedCount });
        }

        log.info("Rollback completed successfully");
    } catch (rollbackErr) {
        log.error("Rollback failed", rollbackErr);
        log.error("Manual cleanup required", { 
            accessControlIds: insertedAccessControlIds, 
            enumerationIds: insertedEnumerationIds 
        });
    }
}

// Print summary at the end
log.summary();
 