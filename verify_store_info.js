const { MongoClient } = require('mongodb');

// MongoDB connection configuration
const mongoUri = 'mongodb://localhost:27017';
const dbName = 'data_axle';
const collectionName = 'data_axle_users';

async function verifyStoreInfo() {
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('🔌 Connected to MongoDB successfully');
        
        const db = client.db(dbName);
        const collection = db.collection(collectionName);
        
        // Get total count
        const totalRecords = await collection.countDocuments();
        console.log(`📊 Total customer email records in database: ${totalRecords}`);
        
        // Count records with store information
        const withStoreId = await collection.countDocuments({ external_store_id: { $exists: true } });
        const withStoreName = await collection.countDocuments({ store_name: { $exists: true } });
        const withBothStoreInfo = await collection.countDocuments({ 
            external_store_id: { $exists: true }, 
            store_name: { $exists: true } 
        });
        
        console.log('\n📈 Store Information Status:');
        console.log('='.repeat(40));
        console.log(`Records with External Store ID: ${withStoreId}`);
        console.log(`Records with Store Name: ${withStoreName}`);
        console.log(`Records with Both Store Info: ${withBothStoreInfo}`);
        console.log(`Records without Store Info: ${totalRecords - withBothStoreInfo}`);
        
        if (withBothStoreInfo > 0) {
            console.log(`✅ Store info coverage: ${((withBothStoreInfo / totalRecords) * 100).toFixed(1)}%`);
        }
        
        // Show sample records
        console.log('\n📋 Sample Records:');
        console.log('='.repeat(40));
        
        // Sample with store info
        if (withBothStoreInfo > 0) {
            console.log('\n🏪 Records WITH store information:');
            const withStoreInfo = await collection.find({ 
                external_store_id: { $exists: true }, 
                store_name: { $exists: true } 
            }).limit(5).toArray();
            
            withStoreInfo.forEach((record, index) => {
                console.log(`${index + 1}. ${record.email}`);
                console.log(`   Store: ${record.store_name} (ID: ${record.external_store_id})`);
                if (record.store_info_updated_at) {
                    console.log(`   Updated: ${record.store_info_updated_at.toLocaleDateString()}`);
                }
                console.log('');
            });
        }
        
        // Sample without store info
        const withoutStoreInfo = await collection.find({ 
            $or: [
                { external_store_id: { $exists: false } },
                { store_name: { $exists: false } }
            ]
        }).limit(3).toArray();
        
        if (withoutStoreInfo.length > 0) {
            console.log('❌ Records WITHOUT store information:');
            withoutStoreInfo.forEach((record, index) => {
                console.log(`${index + 1}. ${record.email}`);
                console.log(`   Has Store ID: ${record.external_store_id ? '✅' : '❌'}`);
                console.log(`   Has Store Name: ${record.store_name ? '✅' : '❌'}`);
                console.log('');
            });
        }
        
        // Show unique stores
        if (withStoreId > 0) {
            console.log('🏬 Unique Stores in Database:');
            console.log('='.repeat(40));
            
            const uniqueStores = await collection.aggregate([
                { $match: { store_name: { $exists: true } } },
                { 
                    $group: { 
                        _id: { 
                            store_name: "$store_name", 
                            external_store_id: "$external_store_id" 
                        }, 
                        count: { $sum: 1 } 
                    } 
                },
                { $sort: { count: -1 } },
                { $limit: 10 }
            ]).toArray();
            
            uniqueStores.forEach((store, index) => {
                console.log(`${index + 1}. ${store._id.store_name} (ID: ${store._id.external_store_id}) - ${store.count} customers`);
            });
        }
        
    } catch (error) {
        console.error('❌ Database error:', error);
    } finally {
        await client.close();
        console.log('\n✅ MongoDB connection closed');
    }
}

// Run the verification
console.log('🔍 Store Information Verification Report');
console.log('🗄️  Database: ' + dbName + '.' + collectionName);
console.log('');

verifyStoreInfo().catch(console.error); 