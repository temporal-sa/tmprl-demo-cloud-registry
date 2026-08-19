FROM gradle:8.7-jdk17 AS build
WORKDIR /app
COPY . .
RUN gradle --no-daemon clean installDist

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/build/install/delivery-matching-service ./
EXPOSE 8291 9102
ENTRYPOINT ["bin/delivery-matching-service"]
