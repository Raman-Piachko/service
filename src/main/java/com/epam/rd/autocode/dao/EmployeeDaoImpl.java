package com.epam.rd.autocode.dao;

import com.epam.rd.autocode.ConnectionSource;
import com.epam.rd.autocode.domain.Department;
import com.epam.rd.autocode.domain.Employee;
import com.epam.rd.autocode.domain.FullName;
import com.epam.rd.autocode.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class EmployeeDaoImpl implements EmployeeDao {
    private static final String SELECT_BY_ID = "SELECT * FROM EMPLOYEE WHERE ID = %s";
    private static final String SELECT_BY_MANAGER = "SELECT * FROM EMPLOYEE WHERE MANAGER = %s";
    private static final String SELECT_BY_DEPARTMENT =
            "SELECT * FROM EMPLOYEE WHERE DEPARTMENT = %s";
    private static final String DELETE = "DELETE FROM EMPLOYEE WHERE ID = %s";
    private static final String INSERT = "insert into EMPLOYEE (ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, HIREDATE," +
            " SALARY, MANAGER, DEPARTMENT) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
    private static final String SELECT_ALL = "SELECT * FROM EMPLOYEE";
    private static final String UPDATE = "update EMPLOYEE set " +
            "FIRSTNAME = '%s', " +
            "LASTNAME = '%s', " +
            "MIDDLENAME = '%s', " +
            "POSITION = '%s', " +
            "HIREDATE = '%s', " +
            "SALARY = '%s', " +
            "MANAGER = '%s', " +
            "DEPARTMENT = '%s', " +
            "' where ID = '%s'";
    private static final String ID = "id";
    private static final String SALARY = "salary";
    private static final String HIREDATE = "hiredate";
    private static final String FIRSTNAME = "firstname";
    private static final String LASTNAME = "lastname";
    private static final String MIDDLENAME = "middlename";
    private static final String POSITION = "position";
    private static final String MANAGER = "manager";
    private static final String DEPARTMENT = "department";


    @Override
    public Employee getById(BigInteger id) {
        Employee employee = null;
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(SELECT_BY_ID, id));

            if (resultSet.next()) {
                employee = createEmployee(resultSet);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employee;
    }


    @Override
    public List<Employee> getAll() {
        List<Employee> employees = new ArrayList<>();
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SELECT_ALL);
            while (resultSet.next()) {
                Employee employee = createEmployee(resultSet);
                employees.add(employee);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employees;
    }

    @Override
    public Employee save(Employee employee) {
        String firstName = employee.getFullName().getFirstName();
        String lastName = employee.getFullName().getLastName();
        String middleName = employee.getFullName().getMiddleName();
        Position position = employee.getPosition();
        LocalDate hiredate = employee.getHired();
        BigDecimal salary = employee.getSalary();
        BigInteger managerId = employee.getManager().getId();
        BigInteger departmentId = employee.getDepartment().getId();
        BigInteger id = employee.getId();

        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();

            if (getById(employee.getId()) != null) {
                statement.executeUpdate(String.format(UPDATE, firstName, lastName, middleName, position, hiredate,
                        salary, managerId, departmentId, id));
            } else {
                statement.executeUpdate(String.format(INSERT, id, firstName, lastName, middleName, position, hiredate,
                        salary, managerId, departmentId, id));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return employee;
    }

    @Override
    public void delete(Employee employee) {
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            statement.executeUpdate(String.format(DELETE, employee.getId().toString()));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Employee> getByDepartment(Department department) {
        List<Employee> employees = new ArrayList<>();
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(SELECT_BY_DEPARTMENT, department.getId()));

            while (resultSet.next()) {
                Employee employee = createEmployee(resultSet);
                employees.add(employee);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employees;
    }

    @Override
    public List<Employee> getByManager(Employee manager) {
        List<Employee> employees = new ArrayList<>();
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(SELECT_BY_MANAGER, manager.getId()));
            while (resultSet.next()) {
                Employee employee = createEmployee(resultSet);
                employees.add(employee);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employees;
    }

    @Override
    public Employee getByIdWithFullChain(BigInteger id) {
        Employee employee = null;
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(SELECT_BY_ID, id));
            if (resultSet.next()) {
                employee = createEmployeeWithFullChain(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employee;
    }

    private Employee createEmployee(ResultSet resultSet) {
        Employee employee;
        try {
            Position position = getPosition(resultSet);
            FullName fullName = getFullName(resultSet);
            BigInteger id = BigInteger.valueOf(resultSet.getInt(ID));
            LocalDate hired = getHired(resultSet);
            BigDecimal salary = resultSet.getBigDecimal(SALARY);
            BigInteger managerId = BigInteger.valueOf(resultSet.getInt(MANAGER));
            Employee manager = getManagerById(managerId);
            BigInteger departmentId = BigInteger.valueOf(resultSet.getInt(DEPARTMENT));
            Department department = new DepartmentDaoImpl().getById(departmentId);

            employee = new Employee(id, fullName, position, hired, salary, manager, department);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employee;
    }

    private Employee getManagerById(BigInteger id) {
        Employee employee = null;
        try {
            Connection connection = ConnectionSource.instance().createConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(SELECT_BY_ID, id));

            if (resultSet.next()) {
                employee = createManager(resultSet);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return employee;
    }

    private Employee createManager(ResultSet resultSet) {
        Employee employee = null;
        try {
            Position position = getPosition(resultSet);
            FullName fullName = getFullName(resultSet);
            BigInteger id = BigInteger.valueOf(resultSet.getInt(ID));
            LocalDate hired = getHired(resultSet);
            BigDecimal salary = resultSet.getBigDecimal(SALARY);
            BigInteger departmentId = BigInteger.valueOf(resultSet.getInt(DEPARTMENT));
            Department department = new DepartmentDaoImpl().getById(departmentId);
            employee = new Employee(id, fullName, position, hired, salary, null, department);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return employee;
    }

    private LocalDate getHired(ResultSet resultSet) throws SQLException {
        return resultSet.getDate(HIREDATE).toLocalDate();
    }

    private FullName getFullName(ResultSet resultSet) throws SQLException {
        String firstName = resultSet.getString(FIRSTNAME);
        String lastName = resultSet.getString(LASTNAME);
        String middleName = resultSet.getString(MIDDLENAME);
        return new FullName(firstName, lastName, middleName);
    }

    private Position getPosition(ResultSet resultSet) throws SQLException {
        return Position.valueOf(resultSet.getString(POSITION).toUpperCase());
    }

    private Employee createEmployeeWithFullChain(ResultSet resultSet) {
        Employee employee;
        try {
            Position position = getPosition(resultSet);
            FullName fullName = getFullName(resultSet);
            BigInteger id = BigInteger.valueOf(resultSet.getInt(ID));
            LocalDate hired = getHired(resultSet);
            BigDecimal salary = resultSet.getBigDecimal(SALARY);
            BigInteger managerId = BigInteger.valueOf(resultSet.getInt(MANAGER));
            Employee manager = getByIdWithFullChain(managerId);
            BigInteger departmentId = BigInteger.valueOf(resultSet.getInt(DEPARTMENT));
            Department department = new DepartmentDaoImpl().getById(departmentId);
            employee = new Employee(id, fullName, position, hired, salary, manager, department);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return employee;
    }
}