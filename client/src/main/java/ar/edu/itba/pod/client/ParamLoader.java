package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.Queries.Query;

import java.util.List;

public class ParamLoader {
    private List<String> addresses;
    private String movementsInPath;
    private String airportsInPath;
    private String outPath;
    private String timeOutPath;
    private int queryNumber;


    public ParamLoader(){
        String queryNumberS     = System.getProperty("query","1");
        try{
            queryNumber = Integer.parseInt(queryNumberS);
        }catch (NumberFormatException e){
            throw new IllegalArgumentException("Illegal query number: "+queryNumber);
        }

        movementsInPath = System.getProperty("movementsInPath","movimientos.csv");
        airportsInPath  = System.getProperty("airportsInPath","aeropuertos.csv");
        outPath         = System.getProperty("outPath","query"+queryNumber+".csv");
        timeOutPath     = System.getProperty("timeOutPath","query"+queryNumber+".txt");
    }

    public List<String> getAddresses() {
        return addresses;
    }


    public String getMovementsInPath() {
        return movementsInPath;
    }

    public String getAirportsInPath() {
        return airportsInPath;
    }

    public String getOutPath() {
        return outPath;
    }

    public String getTimeOutPath() {
        return timeOutPath;
    }

    public int getQueryNumber() {
        return queryNumber;
    }
}
