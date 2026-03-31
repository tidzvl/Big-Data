// ============================================================
//  CO3138 — Dữ liệu lớn | Lab 01 Report
//  Typst source — compile with: typst compile report.typ
// ============================================================

#set page(paper: "a4", margin: (top: 2.5cm, bottom: 2.5cm, left: 2.5cm, right: 2.5cm))
#set text(font: "Times New Roman", size: 13pt, lang: "vi")
#set par(justify: true, leading: 0.8em)
#set heading(numbering: "1.1.")

// ======================= TRANG BÌA ==========================
#page(margin: (top: 2cm, bottom: 2cm, left: 2.5cm, right: 2.5cm))[
  #align(center)[
    #text(size: 14pt, weight: "bold")[
      ĐẠI HỌC QUỐC GIA THÀNH PHỐ HỒ CHÍ MINH \
      TRƯỜNG ĐẠI HỌC BÁCH KHOA \
      KHOA KHOA HỌC & KỸ THUẬT MÁY TÍNH
    ]

    #v(1cm)
    #line(length: 60%, stroke: 0.5pt)
    #v(2cm)

    #text(size: 24pt, weight: "bold")[
      BÁO CÁO LAB 1
    ]
    #v(0.5cm)
    #text(size: 18pt, weight: "bold")[
      Kafka & Spark
    ]

    #v(2cm)

    #text(size: 14pt)[
      *Môn học:* Dữ liệu lớn (CO3138) \
      *Học kì:* HK252 \
      *Giảng viên:* Đoàn Ngô Đức Phương
    ]

    #v(1.5cm)

    #align(center)[
      #table(
        columns: (auto, auto, auto),
        inset: 10pt,
        align: center,
        stroke: 0.5pt,
        [*STT*], [*Họ và tên*], [*MSSV*],
        [1], [Nguyễn Chánh Tín], [2213491],
        [2], [Trần Quốc Toàn], [2213540],
      )
    ]

    #v(1fr)
    #text(size: 13pt)[
      TP. Hồ Chí Minh, tháng 3 năm 2026
    ]
  ]
]

// ===================== MỤC LỤC =============================
#outline(title: [Mục lục], indent: 1.5em)
#pagebreak()

// ===================== NỘI DUNG =============================

= Tổng quan

Bài thí nghiệm Lab 01 nhằm xây dựng kiến thức thực hành về hai nền tảng xử lý dữ liệu phân tán phổ biến: *Apache Spark* và *Apache Kafka*. Cụ thể, bài lab yêu cầu:

+ Triển khai cụm Kafka gồm 3 broker trên môi trường local bằng Docker.
+ Đọc dữ liệu từ Kafka, xử lý lỗi định dạng binary.
+ Phân tích tập dữ liệu MovieLens bằng Spark DataFrame:
  - Tìm top 5 phim có điểm đánh giá trung bình cao nhất (với số lượng rating > 30).
  - Tìm 5 tag tệ nhất (gắn với điểm đánh giá trung bình thấp nhất).
  - Phân tích sâu: các tag đó có thực sự phản ánh chất lượng phim thấp hay không.

= Môi trường thực hiện

#table(
  columns: (auto, 1fr),
  inset: 10pt,
  stroke: 0.5pt,
  [*Thành phần*], [*Chi tiết*],
  [Hệ điều hành], [Windows 11],
  [Python], [3.13],
  [PySpark], [4.0.0],
  [Java], [OpenJDK 17 (Eclipse Temurin)],
  [Docker], [Docker Desktop for Windows],
  [Kafka Image], [apache/kafka:3.9.0],
  [IDE], [VS Code + Jupyter Lab],
)

= Triển khai cụm Kafka

Cụm Kafka được triển khai bằng Docker Compose với 3 broker sử dụng chế độ KRaft (không cần Zookeeper). Mỗi broker đóng vai trò đồng thời là controller và broker, giao tiếp qua 3 listener: `PLAINTEXT` (client), `CONTROLLER` (quorum), và `INTERNAL` (inter-broker).

Các broker được expose ra các port khác nhau trên máy host:
- Broker 1: `localhost:9092`
- Broker 2: `localhost:9192`
- Broker 3: `localhost:9292`

Lệnh triển khai:

```bash
docker compose --profile kafka up -d
```

#figure(
  image("screenshots/11_docker_kafka.png", width: 100%),
  caption: [Cụm Kafka 3 broker đang hoạt động trên Docker.]
)

= Khởi tạo Spark Session

SparkSession được khởi tạo ở chế độ `local[*]` (sử dụng tất cả CPU cores), kèm theo các Kafka connector JARs cần thiết được khai báo qua `spark.jars.packages`.

#figure(
  image("screenshots/01_spark_init.png", width: 100%),
  caption: [Khởi tạo SparkSession thành công — Spark 4.0.0.]
)

= Chuẩn bị dữ liệu

== Tải tập dữ liệu MovieLens

Tập dữ liệu *MovieLens Latest Small* được tải về từ Kaggle thông qua thư viện `kagglehub`. Tập dữ liệu gồm 3 bảng chính:

- *ratings*: Thông tin đánh giá phim (userId, movieId, rating, timestamp).
- *movies*: Thông tin phim (movieId, title, genres).
- *tags*: Tag do người dùng gắn cho phim (userId, movieId, tag, timestamp).

#figure(
  image("screenshots/02_dataset_preview.png", width: 100%),
  caption: [Dữ liệu mẫu từ 3 bảng ratings, movies, tags.]
)

== Tạo Kafka topics và nạp dữ liệu

Ba topic (`ratings`, `movies`, `tags`) được tạo trên cụm Kafka với `replication_factor=3` (mỗi broker giữ một bản sao). Sau đó, dữ liệu được serialize thành JSON và đẩy vào các topic tương ứng thông qua Spark Kafka connector.

#figure(
  image("screenshots/03_kafka_topics.png", width: 100%),
  caption: [Tạo 3 Kafka topics thành công.]
)

#figure(
  image("screenshots/04_kafka_push.png", width: 100%),
  caption: [Đẩy dữ liệu vào Kafka — hiển thị số lượng records mỗi topic.]
)

= Excercise

== Exercise 1 — Đọc dữ liệu từ Kafka và xử lý lỗi định dạng

=== Vấn đề

Khi đọc dữ liệu từ Kafka, cột `value` được trả về dưới dạng *binary* (bytes). Nếu không xử lý, dữ liệu sẽ hiển thị dưới dạng mảng byte không đọc được.

#figure(
  image("screenshots/05_kafka_raw_binary.png", width: 100%),
  caption: [Dữ liệu thô từ Kafka — cột `value` ở dạng binary (lỗi định dạng).]
)

=== Giải pháp

Pipeline xử lý gồm 3 bước:

#table(
  columns: (auto, auto, 1fr),
  inset: 8pt,
  stroke: 0.5pt,
  align: (center, left, left),
  [*Bước*], [*Thao tác*], [*Mục đích*],
  [1], [`CAST(value AS STRING)`], [Chuyển binary → chuỗi JSON (UTF-8)],
  [2], [`from_json(col, schema)`], [Parse chuỗi JSON thành struct có kiểu dữ liệu],
  [3], [`select("data.*")`], [Trải phẳng struct thành các cột riêng biệt],
)

Schema được định nghĩa tường minh cho từng topic để đảm bảo kiểu dữ liệu chính xác (IntegerType, DoubleType, StringType, LongType).

#figure(
  image("screenshots/06_kafka_parsed.png", width: 100%),
  caption: [Dữ liệu sau khi parse — schema đúng, dữ liệu đọc được.]
)

== Exercise 2 — Top 5 phim có điểm đánh giá trung bình cao nhất

=== Phương pháp

+ Nhóm bảng `ratings` theo `movieId`, tính `avg(rating)` và `count(rating)`.
+ Lọc các phim có `rating_count > 30` để đảm bảo tính đại diện thống kê.
+ Join với bảng `movies` để lấy tên phim.
+ Sắp xếp giảm dần theo `avg_rating`, lấy top 5.

=== Kết quả

#figure(
  image("screenshots/07_top5_movies.png", width: 100%),
  caption: [Top 5 phim có điểm đánh giá trung bình cao nhất (rating count > 30).]
)

Điều kiện `rating_count > 30` giúp loại bỏ các phim có quá ít đánh giá — những phim này dễ có điểm trung bình cực đoan (rất cao hoặc rất thấp) nhưng không mang ý nghĩa thống kê.

== Exercise 3 — 5 tag tệ nhất (gắn với điểm đánh giá trung bình thấp nhất)

=== Phương pháp

+ Join bảng `tags` với `ratings` theo `movieId` để gắn rating cho mỗi tag.
+ Chuẩn hóa text tag (lowercase + trim) để gộp các biến thể giống nhau.
+ Nhóm theo `tag_clean`, tính `avg(rating)`.
+ Sắp xếp tăng dần, lấy 5 tag có `avg_rating` thấp nhất.

=== Kết quả

#figure(
  image("screenshots/08_worst5_tags.png", width: 100%),
  caption: [5 tag có điểm đánh giá trung bình thấp nhất.]
)

== Exercise 4 — Phân tích sâu: Các tag tệ có thực sự phản ánh chất lượng phim?

=== Đặt vấn đề

Kết quả Exercise 3 cho thấy một số tag gắn liền với điểm trung bình thấp. Tuy nhiên, câu hỏi đặt ra: *liệu các phim mang những tag đó có thực sự bị đánh giá thấp, hay chỉ là ảnh hưởng của một vài đánh giá cá biệt?*

=== Phương pháp

+ Với mỗi tag trong 5 tag tệ nhất, tìm tất cả các phim mang tag đó.
+ Tính *điểm đánh giá trung bình riêng* của từng phim (từ toàn bộ ratings, không chỉ từ người gắn tag).
+ Đếm số phim mang mỗi tag.
+ So sánh `mean_of_movie_avgs` (trung bình của các trung bình phim) với *điểm trung bình toàn cục* (global average).

=== Kết quả chi tiết

#figure(
  image("screenshots/09_worst_tags_detail.png", width: 100%),
  caption: [Điểm đánh giá trung bình của từng phim mang các tag tệ nhất.]
)

=== Bảng tổng hợp

#figure(
  image("screenshots/10_worst_tags_summary.png", width: 100%),
  caption: [So sánh điểm trung bình các tag tệ nhất với điểm trung bình toàn cục.]
)

=== Nhận xét

- *Nếu `mean_of_movie_avgs` < global average (~3.5):* Phim mang tag đó thực sự có xu hướng bị đánh giá thấp hơn mặt bằng chung. Tag phản ánh đúng một đặc điểm tiêu cực.

- *Nếu `mean_of_movie_avgs` ≈ hoặc > global average:* Điểm thấp ở cấp tag chỉ do một số ít đánh giá cá biệt, không đại diện cho chất lượng thực sự của phim. Tag không phải chỉ báo đáng tin cậy.

- *Số lượng phim (`num_movies`):* Tag chỉ xuất hiện trên 1–2 phim có độ tin cậy rất thấp — kết quả bị chi phối bởi outlier.

*Kết luận:* Điểm đánh giá trung bình thấp ở cấp tag không nhất thiết đồng nghĩa với việc phim mang tag đó kém chất lượng. Mối quan hệ phụ thuộc vào kích thước mẫu và tính nhất quán của các đánh giá thấp trên nhiều phim.

= Kết luận

Qua bài thí nghiệm Lab 01, nhóm đã hoàn thành các mục tiêu:

+ *Triển khai hạ tầng:* Cụm Kafka 3 broker hoạt động ổn định trên Docker với chế độ KRaft.
+ *Xử lý dữ liệu streaming:* Đọc thành công dữ liệu từ Kafka, xử lý lỗi định dạng binary bằng pipeline CAST → from_json → select.
+ *Phân tích dữ liệu:* Sử dụng Spark DataFrame API để thực hiện các truy vấn phân tích trên tập MovieLens — từ aggregation đơn giản đến phân tích đa chiều kết hợp nhiều bảng.
+ *Tư duy phản biện:* Không dừng lại ở kết quả bề mặt, mà kiểm chứng xem các tag tệ có thực sự phản ánh chất lượng phim hay chỉ là hiệu ứng thống kê do mẫu nhỏ.
