from twisted.application.service import ServiceMaker

serviceMaker = ServiceMaker(
    'airtime-service', 'airtime_service.service',
    'RESTful service for issuing airtime.', 'airtime-service')
