// queries.js - Demonstrating CRUD, advanced queries, aggregation, and indexing in MongoDB

const { MongoClient } = require("mongodb");

// Local MongoDB connection
const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri);

async function run() {
  try {
    await client.connect();
    console.log("✅ Connected to MongoDB");

    const db = client.db("plp_bookstore");
    const books = db.collection("books");

    console.log("\n==== Task 1: Basic Finds ====");
    // Find all books in a specific genre
    console.log("Fiction books:");
    console.table(await books.find({ genre: "Fiction" }).toArray());

    // Find books published after a certain year
    console.log("\nBooks published after 1950:");
    console.table(await books.find({ published_year: { $gt: 1950 } }).toArray());

    // Find books by a specific author
    console.log("\nBooks by George Orwell:");
    console.table(await books.find({ author: "George Orwell" }).toArray());

    console.log("\n==== Task 2: Update & Delete ====");
    // Update the price of a specific book
    await books.updateOne({ title: "1984" }, { $set: { price: 15.0 } });
    console.log("✅ Updated price for '1984'");

    // Delete a book by its title
    await books.deleteOne({ title: "Moby Dick" });
    console.log("🗑️ Deleted 'Moby Dick'");

    console.log("\n==== Task 3: Advanced Queries ====");
    // Books in stock AND published after 2010
    console.log("Books in stock after 2010:");
    console.table(await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());

    // Projection
    console.log("\nProjection (title, author, price):");
    console.table(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    // Sorting
    console.log("\nBooks sorted by price ascending:");
    console.table(await books.find().sort({ price: 1 }).toArray());

    console.log("\nBooks sorted by price descending:");
    console.table(await books.find().sort({ price: -1 }).toArray());

    // Pagination (5 per page)
    console.log("\nPage 1 (5 books):");
    console.table(await books.find().skip(0).limit(5).toArray());

    console.log("\nPage 2 (next 5 books):");
    console.table(await books.find().skip(5).limit(5).toArray());

    console.log("\n==== Task 4: Aggregation Pipelines ====");

    // Average price by genre
    console.log("\n📊 Average price by genre:");
    const avgByGenre = await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
      { $sort: { avgPrice: -1 } }
    ]).toArray();
    console.table(avgByGenre);

    // Author with most books
    console.log("\n✍️ Author with most books:");
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.table(topAuthor);

    // Group by decade
    console.log("\n📚 Books grouped by decade:");
    const byDecade = await books.aggregate([
      {
        $group: {
          _id: {
            decade: { $subtract: ["$published_year", { $mod: ["$published_year", 10] }] }
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { "_id.decade": 1 } }
    ]).toArray();
    console.table(byDecade);

    console.log("\n==== Task 5: Indexes ====");
    // Create indexes
    await books.createIndex({ title: 1 });
    await books.createIndex({ author: 1, published_year: 1 });
    console.log("✅ Indexes created!");
  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    await client.close();
    console.log("\n🔌 Connection closed");
  }
}

// Run the function
run().catch(console.error);
