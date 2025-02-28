# 🏆 DataKick

🚀 **DataKick** is a football-focused data analysis platform. The project leverages an open-source stack to collect, transform, and visualize data on matches, players, and teams.

## 📌 Overview

The goal of **DataKick** is to provide a scalable solution for football data analysis, enabling:

- Data collection from reliable sources 📊
- Efficient data processing and storage ⚙️
- Interactive dashboards for visualization 📈

## 🏗️ Architecture and Stack

The project follows an **ETL + Data Visualization** model, utilizing the following technologies:

| Component           | Technology                      |
| ------------------- | ------------------------------- |
| **Orchestration**   | Apache Airflow + AstroSDK       |
| **Data Collection** | APIs: SportsMonk, Football Data |
| **Storage**         | MinIO (Amazon S3 equivalent)    |
| **Transformation**  | DuckDB                          |
| **Visualization**   | Metabase                        |

## 🚀 Setup and Execution

### 🔧 **1. Prerequisites**

Ensure you have the following installed:

- Docker and Docker Compose
- Python 3.9+
- Node.js (if running additional services)

### ⚙️ **2. Installation**

Clone the repository:

```bash
git clone https://github.com/arthursgonzaga/DataKick.git
cd DataKick
```

Start the services using **Docker Compose**:

```bash
docker-compose up -d
```

### 🏗 **3. Airflow Configuration**

For the first-time setup, initialize Airflow’s database:

```bash
docker-compose run --rm airflow-init
```

Access Airflow at:

```plaintext
http://localhost:8080
```

**Username:** `admin`  
**Password:** `admin`

### 📊 **4. Accessing Metabase**

Metabase will be available at:

```
http://localhost:3000
```

## 📌 Usage Example

Once set up, you can execute ETL pipelines in Airflow and visualize the data in Metabase.

## 🤝 Contributing

We welcome contributions! To contribute:

1. Fork the repository 🍴
2. Create a feature branch (`git checkout -b my-feature`)
3. Commit your changes (`git commit -m 'My feature'`)
4. Push to your branch (`git push origin my-feature`)
5. Open a Pull Request 🚀

## 📬 Contact

For questions or suggestions, reach out:

- 📧 Email: [arthursgonzaga@gmail.com](mailto:arthursgonzaga@gmail.com)
- 🔗 LinkedIn: [linkedin.com/in/arthursgonzaga](https://linkedin.com/in/arthursgonzaga)

---

📌 **Stay updated** ⭐ If you like this project, don't forget to **star** the repository!
