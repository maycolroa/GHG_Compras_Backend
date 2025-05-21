import { Controller, Get, Post, Body, UseGuards, Req} from '@nestjs/common';
import { UserService } from './user.service';
import { CreateUserDto } from './dto/create-user.dto';
import { LoginUserDto } from './dto/login-user.dto';
import { AuthGuard } from '@nestjs/passport';
import { GetUser } from './decorators/get-user.decorator';
import { User } from './entities/user.entity';
import { request } from 'http';




@Controller('user')
export class UserController {
  constructor(private readonly userService: UserService) {}
  // Ruta para crear usuarios
  @Post('register')
  create(@Body() createUserDto: CreateUserDto) {
    return this.userService.create(createUserDto);
  }
  // ruta para login 
  @Post('login')
  loginUser(@Body() loginUserDto: LoginUserDto) {
    return this.userService.loginUser(loginUserDto);
  
  }

  // Autenticacion 
  @Get('private')
  @UseGuards(AuthGuard())
  testingPrivateRoute(@Req() req) {
    return { message: 'Verificando usuario en consola', user: req.user };
  }
  
}