const fs = require('fs');
const csv = require('csv-parser');
const axios = require('axios');
const { MongoClient } = require('mongodb');

// MongoDB connection string
const mongoUri = 'mongodb://localhost:27017';
const dbName = 'data_axle';
const collectionName = 'data_axle_users';

// Data Axle API configuration
const DATA_AXLE_API = 'https://api.data-axle.com/v2/people/match';
const API_TOKEN = 'd28a19c1f4168ccb42f67c2b';

async function processEmails(startRange, endRange) {
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('Connected to MongoDB');
        
        const db = client.db(dbName);
        const collection = db.collection(collectionName);
        
        let currentIndex = 0;
        const rows = [];
        
        // First collect all rows
        await new Promise((resolve, reject) => {
            fs.createReadStream('customer_emails.csv')
                .pipe(csv())
                .on('data', (row) => {
                    rows.push(row);
                })
                .on('end', resolve)
                .on('error', reject);
        });

        count =0;

        // Then process them sequentially
        for (const row of rows) {
            currentIndex++;
            if (currentIndex >= startRange && currentIndex <= endRange) {
                try {
                    const email = row.customer_email.toLowerCase();
                    
                    // Check if email already exists in the database
                    const existingRecord = await collection.findOne({ email: email });
                    if (existingRecord) {
                        console.log(`Email ${email} already processed. Skipping...`);
                        continue; // Skip to the next email
                    }
                    
                    // Call Data Axle API
                    const response = await axios.post(DATA_AXLE_API, {
                        identifiers: {
                            email: email
                        },
                        packages: ["core_v1", "interests_v2", "enhanced_v2"]
                    }, {
                        headers: {
                            'X-AUTH-TOKEN': API_TOKEN,
                            'Content-Type': 'application/json'
                        }
                    });
                    
                    // Store in MongoDB
                    await collection.insertOne({
                        email: email,
                        data: response.data,
                        processedAt: new Date()
                    });
                    
                    console.log(`Processed ${currentIndex}: ${email}`);
                    
                    // Add delay to avoid rate limiting
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    
                } catch (error) {
                    console.error(`Error processing email ${row.email}:`, error.message);
                }
            }
        }
        
    } catch (error) {
        console.error('Error:', error);
    } finally {
        await client.close();
        console.log('MongoDB connection closed');
    }
}

// For testing with first 2 emails
processEmails(1, 400);