import { Injectable, UnauthorizedException } from '@nestjs/common';
import { CreateUserDto } from './dto/create-user.dto';
import { Repository } from 'typeorm';
import * as bcrypt from 'bcrypt';
import { User } from './entities/user.entity';
import { InjectRepository } from '@nestjs/typeorm';
import { JwtService } from '@nestjs/jwt';
import { LoginUserDto } from './dto/login-user.dto';
import { JwtPayload } from './interfaces/jwt-payload.interface';

@Injectable()
export class UserService {
  constructor(
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
    private readonly jwtService: JwtService,
  ) {}
  
  // funcion para crear usuarios
  async create(createUserDto: CreateUserDto) {
    try {
      const { password, ...userData } = createUserDto
      const user = this.userRepository.create({
        ...userData,
        password: bcrypt.hashSync(password, 10)
      });
      await this.userRepository.save(user);
      delete user.password
     return {
      ...user,
      token: this.getJwtToken({ id: user.id })
    }; 
    } catch (error) {
      console.log(error);
    }
  }
  
  // funcion para logiarse
  async loginUser(loginUserDto: LoginUserDto) {
    const { email, password } = loginUserDto;
    const user = await this.userRepository.findOne({
      where: { email },
      select: { password: true, email: true, id: true },
    });
    
    if (!user) {
      throw new UnauthorizedException('usuario incorrecto');
    }
    
    if (!bcrypt.compareSync(password, user.password)) {
      throw new UnauthorizedException('contraseña incorrecto')
    }

    // verificacion de incriptacion jwk
    return {
      ...user,
      token: this.getJwtToken({ id: user.id })
    }; 
  }

  private getJwtToken(payload: JwtPayload) {
    const token = this.jwtService.sign(payload);
    return token;
  }
}