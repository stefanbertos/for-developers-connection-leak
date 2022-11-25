package com.example.demo.service;

import com.example.demo.vo.Payment;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DataService {
    private DataSource dataSource;

    @Autowired
    public DataService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void processData(String filePath) throws IOException, CsvValidationException, SQLException {
        int rowIndex = 0;
        List<Payment> list = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(Paths.get(filePath))) {
            try (CSVReader csvReader = new CSVReader(reader); ) {
                csvReader.skip(1);
                String[] record;
                while ((record = csvReader.readNext()) != null) {
                    rowIndex++;
                    Payment payment = new Payment();
                    payment.setStep(Integer.parseInt(record[0]));
                    payment.setType(record[1]);
                    payment.setAmount(Double.valueOf(record[2]));
                    payment.setNameOrig(record[3]);
                    payment.setOldBalanceOrg(Double.valueOf(record[4]));
                    payment.setNewBalanceOrig(Double.valueOf(record[5]));
                    payment.setNameDest(record[6]);
                    payment.setOldBalanceDest(Double.valueOf(record[7]));
                    payment.setNewBalanceDest(Double.valueOf(record[8]));
                    payment.setIsFraud(Integer.parseInt(record[9]));
                    payment.setIsFlaggedFraud(Integer.parseInt(record[10]));
                    list.add(payment);

                    if (rowIndex % 10000 == 0) {
                        log.info("processing row {}", rowIndex);
                        batchInsert(list);
                        list.clear();
                    }
                }
                if (list.size() != 0) {
                    batchInsert(list);
                }

            }
        }
    }

    private void batchInsert(List<Payment> list) throws SQLException {
        Connection connection = dataSource.getConnection();
        String insertEmployeeSQL = "INSERT /*+append */ INTO payment(step, type, amount, nameorig, oldbalanceorg, newbalanceorig, namedest, oldbalancedest, newbalancedest, isfraud, isflaggedfraud) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement employeeStmt = connection.prepareStatement(insertEmployeeSQL);
        for (int i = 0; i < list.size(); i++) {
            employeeStmt.setInt(1, list.get(i).getStep());
            employeeStmt.setString(2, list.get(i).getType());
            employeeStmt.setDouble(3, list.get(i).getAmount());
            employeeStmt.setString(4, list.get(i).getNameOrig());
            employeeStmt.setDouble(5, list.get(i).getOldBalanceOrg());
            employeeStmt.setDouble(6, list.get(i).getNewBalanceOrig());
            employeeStmt.setString(7, list.get(i).getNameDest());
            employeeStmt.setDouble(8, list.get(i).getOldBalanceDest());
            employeeStmt.setDouble(9, list.get(i).getNewBalanceDest());
            employeeStmt.setInt(10, list.get(i).getIsFraud());
            employeeStmt.setInt(11, list.get(i).getIsFlaggedFraud());
            employeeStmt.addBatch();
        }

        employeeStmt.executeBatch();
        employeeStmt.close();
       // connection.close();
    }
}
