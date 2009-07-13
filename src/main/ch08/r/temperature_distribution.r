png("temperature_distribution.png")
data <- read.table("output_sorted")
plot(data, xlab="Temperature", ylab="Number of readings")
dev.off()