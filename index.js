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
        const users = database.collection('users');
        const students = database.collection('students');
        const sales = database.collection('sales');
        const orders = database.collection('orders');
        const customers = database.collection('customers');
        const arrays = database.collection('arrays');




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
                    $group: { _id: '$subject', totalStudents: { $sum: 1 } }
                }
            ]).toArray();

            res.send(result)
        })


        //Highest Scorer to Lower Score
        app.get('/highest-score', async (req, res) => {
            const result = await students.aggregate([
                {
                    $group: { _id: '$name', totalMark: { $sum: '$score' } }
                },
                {
                    $sort: { totalMark: -1 }
                }
            ]).toArray()
            res.send(result)
        })

        //Total Quantity Sold
        app.get('/total-quantity', async (req, res) => {
            const result = await sales.aggregate([
                {
                    $group: { _id: null, totalQuantity: { $sum: '$quantity' } }
                }
            ]).toArray();

            if (result.length === 0) {
                res.status(404).send('No sales data found')
            }

            res.send(result)
        })


        //Total Revenue by Product
        app.get('/total-revenue', async (req, res) => {
            const result = await sales.aggregate([
                {
                    $addFields: { total: { $multiply: ['$quantity', '$price'] } }
                },
                {
                    $group: { _id: '$product', totalSum: { $sum: '$total' } }
                }

            ]).toArray();

            res.send(result)
        })


        // Total Sales for Each Product
        app.get('/sold-each-product', async (req, res) => {
            const result = await sales.aggregate([
                {
                    $group: { _id: '$product', totalSales: { $sum: '$quantity' } }
                }
            ]).toArray();

            if (result.length === 0) {
                res.status(404).send('No data found')
            }

            res.send(result)
        })

        //Top-Selling Product
        app.get('/top-selling-product', async (req, res) => {
            const result = await sales.aggregate([
                {
                    $group: { _id: '$product', totalSales: { $sum: '$quantity' } }
                },
                {
                    $sort: { totalSales: -1 }
                }
            ]).toArray();

            if (result.length === 0) {
                res.status(404).send('No data found')
            }


            res.send(result)
        })


        /* $Project */

        //Simple inclusion and exclusion
        app.get('/basic', async (req, res) => {
            const result = await sales.aggregate([
                {
                    $project: { product: 1, price: 1, _id: 0 }
                }
            ]).toArray();

            res.send(result)
        })


        //Renaming a field
        app.get('/remain-field', async (req, res) => {
            const result = await sales.aggregate([
                {
                    $project: { product: 1, saleId: 1, saleIDNO: '$saleId', productPrice: '$price' }
                }
            ]).toArray();

            res.send(result)
        })

        //Creating a new field

        app.get('/new-field', async (req, res) => {
            const result = await sales.aggregate([
                {
                    $project: { product: 1, saleId: 1, price: 1, discount: { $multiply: ['$price', 0.1] } }
                }
            ]).toArray();

            res.send(result)
        })


        app.get('/orders', async (req, res) => {
            const result = await orders.aggregate([
                {
                    $project: {
                        _id: 0,
                        orderId: 1,
                        customerName: 1,
                        totalAmount: 1,
                        shippingFee: {
                            $multiply: ['$totalAmount', 0.1]
                        }

                    }
                }
            ]).toArray()

            res.send(result)
        })

        app.get('/increase-age', async (req, res) => {
            const result = await users.aggregate([
                {
                    $project: { name: 1, _id: 0, oldAge: '$age', newAge: { $sum: ['$age', 2] } }
                }
            ]).toArray()

            res.send(result)
        })


        //Extracting Nested object Fields
        app.get('/nested-objects', async (req, res) => {
            const result = await customers.aggregate([
                {
                    $project: {
                        orderId: 1,
                        _id: 0,
                        'customer.email': 1,
                        'customer.name': 1,
                        'customer.phone': 1,
                        renameField: '$orderId',
                        modify: { $sum: ['$totalAmount', 2] },
                        hello: 'I am good'
                    }
                }
            ]).toArray()

            res.send(result)
        })

        //Include, Exclude Specific Fields from an Array
        app.get('/array-stuff', async (req, res) => {
            const result = await arrays.aggregate([
                {
                    $project: {
                        items: {
                            product: 1,
                            price: 1
                        },
                        _id: 0

                    }
                }
            ]).toArray();

            res.send(result)
        })


        //Adding the Number of Items
        app.get('/adding-new', async (req, res) => {
            const result = await arrays.aggregate([
                {
                    $project: {
                        orderId: 1,
                        totalPrice: { $sum: '$items.price' },
                        totalItems: { $size: '$items' }
                    }
                }
            ]).toArray();

            res.send(result)
        })


        //Adding a Discount Field
        app.get('/discount', async (req, res) => {
            const result = await arrays.aggregate([
                {
                    $unwind: '$items'
                },
                {
                    $project: {
                        'items.price': 1,
                        'items.discount': {
                            $cond: {
                                if: { $gte: ['$items.price', 100] },
                                then: 10,
                                else: 0
                            }
                        }
                    }
                },
                {
                    $group: {
                        _id: '$_id', items: { $push: '$items' }
                    }
                }


            ]).toArray();
            res.send(result)
        })


        //Finding the Total Price of Each Item in an Order
        app.get('/total-price-one', async (req, res) => {
            const result = await arrays.aggregate([
                {
                    $unwind: '$items'
                },
                {
                    // inside the items
                    // $project: {
                    //     'items.product': 1,
                    //     'items.totalAmount': {$sum: '$items.quantity'},
                    //     'items.totalPrice': {$multiply: ['$items.quantity','$items.price']}
                    // },

                    $project: {
                        product: '$items.product',
                        orderId: 1,
                        totalAmount: { $multiply: ['$items.quantity', '$items.price'] }
                    }
                }
            ]).toArray();

            res.send(result)
        })


        //Add a Discount Field but Keep Everything Else
        app.get('/discount-price-using-addFields', async (req, res) => {
            const result = await arrays.aggregate([
                {
                    $unwind: '$items'
                },
                {
                    $addFields: {
                        items: {
                            discount: {
                                $cond: {
                                    if: { $gte: ['$items.price', 100] },
                                    then: 10,
                                    else: 0
                                }

                            }
                        }
                    }
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