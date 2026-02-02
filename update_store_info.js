const fs = require('fs');
const csv = require('csv-parser');
const { MongoClient } = require('mongodb');

// MongoDB connection configuration
const mongoUri = 'mongodb://localhost:27017';
const dbName = 'data_axle';
const collectionName = 'data_axle_users';

async function updateStoreInfo() {
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('Connected to MongoDB successfully');
        
        const db = client.db(dbName);
        const collection = db.collection(collectionName);
        
        // Statistics tracking
        let totalProcessed = 0;
        let updatedRecords = 0;
        let notFoundRecords = 0;
        let errorRecords = 0;
        
        console.log('Reading customer emails CSV file...');
        
        // Process CSV file
        const csvRows = [];
        await new Promise((resolve, reject) => {
            fs.createReadStream('customer_emails.csv')
                .pipe(csv())
                .on('data', (row) => {
                    csvRows.push(row);
                })
                .on('end', () => {
                    console.log(`Loaded ${csvRows.length} records from CSV`);
                    resolve();
                })
                .on('error', reject);
        });
        
        console.log('\nStarting MongoDB update process...');
        console.log('='.repeat(50));
        
        // Process each CSV row
        for (const row of csvRows) {
            totalProcessed++;
            
            try {
                const email = row.customer_email?.toLowerCase().trim();
                const externalStoreId = row.external_store_id?.trim();
                const storeName = row.name?.trim();
                
                if (!email) {
                    console.log(`Row ${totalProcessed}: Missing email, skipping...`);
                    errorRecords++;
                    continue;
                }
                
                // Find the existing record in MongoDB
                const existingRecord = await collection.findOne({ email: email });
                
                if (existingRecord) {
                    // Prepare update data
                    const updateData = {};
                    
                    if (externalStoreId) {
                        updateData.external_store_id = externalStoreId;
                    }
                    
                    if (storeName) {
                        updateData.store_name = storeName;
                    }
                    
                    // Add timestamp for tracking
                    updateData.store_info_updated_at = new Date();
                    
                    // Update the record
                    const result = await collection.updateOne(
                        { email: email },
                        { 
                            $set: updateData
                        }
                    );
                    
                    if (result.modifiedCount > 0) {
                        updatedRecords++;
                        console.log(`✅ Updated: ${email} -> Store ID: ${externalStoreId}, Store: ${storeName}`);
                    } else {
                        console.log(`⚠️  No changes needed for: ${email}`);
                    }
                    
                } else {
                    notFoundRecords++;
                    console.log(`❌ Email not found in DB: ${email}`);
                }
                
                // Progress indicator every 100 records
                if (totalProcessed % 100 === 0) {
                    console.log(`\n📊 Progress: ${totalProcessed}/${csvRows.length} processed`);
                    console.log(`   ✅ Updated: ${updatedRecords} | ❌ Not Found: ${notFoundRecords} | 🚫 Errors: ${errorRecords}\n`);
                }
                
            } catch (error) {
                errorRecords++;
                console.error(`❌ Error processing row ${totalProcessed}:`, error.message);
            }
        }
        
        // Final summary
        console.log('\n' + '='.repeat(60));
        console.log('📊 FINAL SUMMARY');
        console.log('='.repeat(60));
        console.log(`Total CSV records processed: ${totalProcessed}`);
        console.log(`✅ Successfully updated: ${updatedRecords}`);
        console.log(`❌ Emails not found in DB: ${notFoundRecords}`);
        console.log(`🚫 Error records: ${errorRecords}`);
        console.log(`📈 Success rate: ${((updatedRecords / totalProcessed) * 100).toFixed(1)}%`);
        
        // Show sample of updated records
        console.log('\n📋 Sample of updated records:');
        const sampleUpdated = await collection.find(
            { 
                external_store_id: { $exists: true },
                store_info_updated_at: { $exists: true }
            }
        ).limit(5).toArray();
        
        sampleUpdated.forEach((record, index) => {
            console.log(`${index + 1}. ${record.email} -> ${record.store_name} (ID: ${record.external_store_id})`);
        });
        
    } catch (error) {
        console.error('❌ Database error:', error);
    } finally {
        await client.close();
        console.log('\n✅ MongoDB connection closed');
        console.log('🎉 Store information update process completed!');
    }
}

// Run the script
console.log('🚀 Starting Store Information Update Script');
console.log('📄 Reading from: customer_emails.csv');
console.log('🗄️  Updating: MongoDB collection data_axle_users');
console.log('');

updateStoreInfo().catch(console.error); 