const express = require('express');
const app = express();
const { MongoClient, ServerApiVersion, ObjectId } = require('mongodb');
require('dotenv').config()
var cors = require('cors')
const port = process.env.PORT || 5000;

//Middleware
app.use(cors());
app.use(express.json())



const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@cluster0.iam7h.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0`;

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
    serverApi: {
        version: ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
    }
});

async function run() {
    try {
        // Connect the client to the server	(optional starting in v4.7)
        await client.connect();

        const database = client.db('Pipeline');
        const users = database.collection('users')
        const students = database.collection('students')




        /* $match: 

        ** Used the query, when directly fetching data;
        ** Used $match, When need to combine filtering with other stages like $group, $project, $sort, etc.;

         */

        /*     app.get('/users', async (req, res) => {
                // const query = {country: 'USA', age: {$gt: 30}}
                // const result = await users.find(query).toArray()
    
                const result = await users.aggregate([
                    {
                        $match: { country: "Canada", age: { $gt: 30 } }
                    }
                ]).toArray()
    
                res.send(result)
            })
    
    
            //Task 1: Total Score for All Students
            app.get('/total', async (req, res) => {
                const result = await students.aggregate([
                    {
                        $group: { _id: null, totalCounts: { $sum: '$score' } }
                    }
                ]).toArray();
    
                res.send({ result })
            })
    
            //Task 2: Total Score by Student
            app.get('/total-score-by-student', async (req, res) => {
                const result = await students.aggregate([
                    {
                        $group: { _id: '$name', score: { $sum: '$score' } }
                    }
                ]).toArray();
    
                res.send({ result })
            }) */




        app.get('/marks', async (req, res) => {
            const result = await students.aggregate([
                {
                    $group: { _id: '$name', totalSubject: { $sum: 1 }, totalMark: { $sum: '$score' } }
                }
            ]).toArray()
            res.send(result)
        })


        // Total Score by Subject
        app.get('/subjectMarks', async (req, res) => {
            const result = await students.aggregate([
                {
                    $group: { _id: '$subject', totalSubjectsMark: { $sum: '$score' } }
                }
            ]).toArray()
            res.send(result)
        })



        // Total Students per Subject
        app.get('/find-total-students-per-subject', async (req, res) => {
            const result = await students.aggregate([
                {
                    $group: { _id: '$subject', totalStudents: {$sum: 1} }
                }
            ]).toArray();

            res.send(result)
        })




        // Send a ping to confirm a successful connection
        await client.db("admin").command({ ping: 1 });
        console.log("Pinged your deployment. You successfully connected to MongoDB!");
    } finally {
        // Ensures that the client will close when you finish/error
        // await client.close();
    }
}
run().catch(console.dir);





app.get('/', async (req, res) => {
    res.send('Aggregate Pipeline')
})


app.listen(port, () => {
    console.log(`This server open by ${port}`)
})