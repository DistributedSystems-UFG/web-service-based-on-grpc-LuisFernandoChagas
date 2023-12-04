from concurrent import futures
import logging

import grpc
import EmployeeService_pb2
import EmployeeService_pb2_grpc

import const

empDB=[
 {
 'id':101,
 'name':'Saravanan S',
 'title':'Technical Leader'
 'salary': 1039
 },
 {
 'id':201,
 'name':'Rajkumar P',
 'title':'Sr Software Engineer'
 'salary': 5500
 }
 ]

class EmployeeServer(EmployeeService_pb2_grpc.EmployeeServiceServicer):

  def CreateEmployee(self, request, context):
    dat = {
    'id':request.id,
    'name':request.name,
    'title':request.title
    'salary': request.salary
    }
    empDB.append(dat)
    return EmployeeService_pb2.StatusReply(status='OK')

  # Endpoint changed if Employee not found
  def GetEmployeeDataFromID(self, request, context):
    usr = [emp for emp in empDB if emp['id'] == request.id]
    if usr:
        return EmployeeService_pb2.EmployeeData(id=usr[0]['id'], name=usr[0]['name'], title=usr[0]['title'])
    else:
        # Employee not found
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details('Employee not found')
        return EmployeeService_pb2.EmployeeData()

  # Endpoint 1
  def GetAverageWage(self, request, context):
    total_salary = sum(int(emp['salary']) for emp in empDB)
    average_salary = total_salary / len(empDB) if len(empDB) > 0 else 0
    return EmployeeService_pb2.AverageWage(average_wage=average_salary)

  # Endpoint 2
  def GetHighWage(self, request, context):
    max_salary_employee = max(empDB, key=lambda x: int(x['salary']))
    emp_data = EmployeeService_pb2.EmployeeData(
        id=max_salary_employee['id'],
        name=max_salary_employee['name'],
        title=max_salary_employee['title'],
        salary=max_salary_employee['salary']
    )
    return EmployeeService_pb2.HighWage(employee_data=emp_data)

  def UpdateEmployeeTitle(self, request, context):
    usr = [ emp for emp in empDB if (emp['id'] == request.id) ]
    usr[0]['title'] = request.title
    return EmployeeService_pb2.StatusReply(status='OK')

  def DeleteEmployee(self, request, context):
    usr = [ emp for emp in empDB if (emp['id'] == request.id) ]
    if len(usr) == 0:
      return EmployeeService_pb2.StatusReply(status='NOK')

    empDB.remove(usr[0])
    return EmployeeService_pb2.StatusReply(status='OK')

  def ListAllEmployees(self, request, context):
    list = EmployeeService_pb2.EmployeeDataList()
    for item in empDB:
      emp_data = EmployeeService_pb2.EmployeeData(id=item['id'], name=item['name'], title=item['title']) 
      list.employee_data.append(emp_data)
    return list

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    EmployeeService_pb2_grpc.add_EmployeeServiceServicer_to_server(EmployeeServer(), server)
    server.add_insecure_port('[::]:'+const.PORT)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
