package com.epam.rd.autocode.dao;

public class DaoFactory {
    public EmployeeDao employeeDAO() {
        return new EmployeeDaoImpl(departmentDAO());
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDaoImpl();
    }
}