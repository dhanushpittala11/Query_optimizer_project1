#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <algorithm>
#include <cstdlib> // For rand() and srand()
#include <ctime>   // For time()
#include <climits> // For INT_MAX

// Simulated database schema
struct User {
    int user_id;
    std::string name;
    int age;
};

struct Order {
    int order_id;
    int user_id;
    std::string product;
    int quantity;
};

// Simulated distributed nodes
struct Node {
    std::vector<User> users;
    std::vector<Order> orders;
};

// Query Optimizer Class
class QueryOptimizer {
private:
    std::vector<Node> nodes;

public:
    QueryOptimizer(const std::vector<Node>& nodes) : nodes(nodes) {}

    // Estimate the cost of a query plan
    int estimateCost(const std::string& plan) {
        if (plan == "plan1") {
            // Cost based on the total number of users across all nodes
            int total_users = 0;
            for (const auto& node : nodes) {
                total_users += node.users.size();
            }
            return total_users;
        } else if (plan == "plan2") {
            // Cost based on the total number of orders across all nodes
            int total_orders = 0;
            for (const auto& node : nodes) {
                total_orders += node.orders.size();
            }
            return total_orders;
        } else {
            return INT_MAX; // Invalid plan
        }
    }

    // Optimize the query by selecting the plan with the lowest cost
    std::pair<std::string, int> optimizeQuery(const std::string& query) {
        int plan1_cost = estimateCost("plan1");
        int plan2_cost = estimateCost("plan2");

        if (plan1_cost < plan2_cost) {
            return {"plan1", plan1_cost};
        } else {
            return {"plan2", plan2_cost};
        }
    }
};

// Query Execution Function
std::vector<std::map<std::string, std::string>> executeQuery(const std::string& plan, const std::vector<Node>& nodes) {
    std::vector<std::map<std::string, std::string>> result;

    if (plan == "plan1") {
        // Simulate joining users and orders from all nodes
        for (const auto& node : nodes) {
            for (const auto& user : node.users) {
                for (const auto& order : node.orders) {
                    if (user.user_id == order.user_id) {
                        std::map<std::string, std::string> row;
                        row["user_id"] = std::to_string(user.user_id);
                        row["name"] = user.name;
                        row["age"] = std::to_string(user.age);
                        row["order_id"] = std::to_string(order.order_id);
                        row["product"] = order.product;
                        row["quantity"] = std::to_string(order.quantity);
                        result.push_back(row);
                    }
                }
            }
        }
    } else if (plan == "plan2") {
        // Simulate a different plan (e.g., filtering orders with quantity > 1)
        for (const auto& node : nodes) {
            for (const auto& order : node.orders) {
                if (order.quantity > 1) {
                    for (const auto& user : node.users) {
                        if (user.user_id == order.user_id) {
                            std::map<std::string, std::string> row;
                            row["user_id"] = std::to_string(user.user_id);
                            row["name"] = user.name;
                            row["age"] = std::to_string(user.age);
                            row["order_id"] = std::to_string(order.order_id);
                            row["product"] = order.product;
                            row["quantity"] = std::to_string(order.quantity);
                            result.push_back(row);
                        }
                    }
                }
            }
        }
    }

    return result;
}

// Function to generate random names
std::string generateRandomName() {
    static const std::string firstNames[] = {"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack"};
    static const std::string lastNames[] = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"};
    return firstNames[rand() % 10] + " " + lastNames[rand() % 10];
}

// Function to generate random products
std::string generateRandomProduct() {
    static const std::string products[] = {"Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Printer", "Headphones", "Camera", "Speaker"};
    return products[rand() % 10];
}

// Function to generate random data for users and orders
void generateRandomData(std::vector<Node>& nodes, int numUsers, int numOrders) {
    srand(time(0)); // Seed the random number generator

    // Generate users
    for (int i = 1; i <= numUsers; ++i) {
        User user = {i, generateRandomName(), 18 + rand() % 50}; // Age between 18 and 67
        nodes[i % nodes.size()].users.push_back(user); // Distribute users across nodes
    }

    // Generate orders
    for (int i = 1; i <= numOrders; ++i) {
        Order order = {i, 1 + rand() % numUsers, generateRandomProduct(), 1 + rand() % 5}; // Quantity between 1 and 5
        nodes[i % nodes.size()].orders.push_back(order); // Distribute orders across nodes
    }
}

int main() {
    // Simulate distributed data across 4 nodes
    std::vector<Node> nodes(4); // 4 nodes

    // Generate 100 users and 200 orders
    generateRandomData(nodes, 100, 200);

    // Create a query optimizer
    QueryOptimizer optimizer(nodes);

    // Optimize the query
    std::string query = "SELECT * FROM users JOIN orders ON users.user_id = orders.user_id";
    auto [best_plan, cost] = optimizer.optimizeQuery(query);

    std::cout << "Best Plan: " << best_plan << ", Estimated Cost: " << cost << std::endl;

    // Execute the query using the best plan
    auto result = executeQuery(best_plan, nodes);

    // Print the query result (limit to 10 rows for readability)
    std::cout << "Query Result (First 10 Rows):" << std::endl;
    for (size_t i = 0; i < std::min(result.size(), size_t(10)); ++i) {
        for (const auto& [key, value] : result[i]) {
            std::cout << key << ": " << value << ", ";
        }
        std::cout << std::endl;
    }

    return 0;
}