import { Module } from '@nestjs/common';
import { UserService } from './user.service';
import { UserController } from './user.controller';
import { TypeOrmModule } from '@nestjs/typeorm';
import { User } from './entities/user.entity';
import { PassportModule } from '@nestjs/passport';
import { JwtModule } from '@nestjs/jwt';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtStrategy } from './strategies/jwt.strategy';



@Module({
  controllers: [UserController],
  providers: [UserService, JwtStrategy ],
  imports: [
    ConfigModule,
    TypeOrmModule.forFeature([User]),
    PassportModule.register({ defaultStrategy: 'jwt' }),

    
  JwtModule.registerAsync({
    imports: [ ConfigModule ], 
    inject: [ConfigService],
    useFactory: ( ConfigService: ConfigService) =>{
      
      return{
        secret: ConfigService.get('JWT_SECRET'),
        signOptions: {
        expiresIn:'2h'
      }
      }
    }
  })
],
  exports: [ TypeOrmModule, JwtStrategy, PassportModule, JwtModule ]
})
export class UserModule {}