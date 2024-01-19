package unimore.iot.architectures.tirocinio.hono.model;

import java.util.Random;

/**
 * @author Riccardo Prevedi
 * @created 25/02/2023 - 21:19
 * @project architectures-iot
 */

public class EngineTemperatureSensor {

    private Random rnd;

    private double temperatureValue;

    public EngineTemperatureSensor() {
        this.rnd = new Random(System.currentTimeMillis());
        this.temperatureValue = 0.0;
    }

    private void generateEngineTemperature() {
        temperatureValue =  80 + rnd.nextDouble() * 20.0;
    }

    public double getTemperatureValue() {
        generateEngineTemperature();
        return temperatureValue;
    }

    public void setTemperatureValue(double temperatureValue) {
        this.temperatureValue = temperatureValue;
    }

    @Override
    public String toString() {
        return "EngineTemperatureSensor [temperatureValue=" + temperatureValue + "]";
    }
}
